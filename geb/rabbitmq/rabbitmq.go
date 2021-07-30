package rabbitmq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/proemergotech/errors"
	"github.com/proemergotech/geb-client/v2/geb"
	"github.com/streadway/amqp"
)

const extraPrefetch = 20
const exchangeName = "events"
const maxPendingPublishes = 100

type Handler struct {
	consumerName string
	username     string
	password     string
	host         string
	port         int
	timeout      time.Duration
	vhost        string

	connection       *amqp.Connection
	connectionMu     *sync.RWMutex
	disconnectWg     *sync.WaitGroup
	disconnectCh     chan struct{}
	sharedCloseCh    chan error
	connectionClosed *uint32

	consumeWg       *sync.WaitGroup
	consumeChannels []*amqp.Channel

	publishCh      chan *publish
	publishCloseCh chan struct{}
	publishWg      *sync.WaitGroup
	amqpPublishCh  *amqp.Channel
	publishConfirm chan amqp.Confirmation

	shutdownWg *sync.WaitGroup
	shutdownCh chan struct{}

	subscribes    map[string]*subscribe
	onError       func(err error)
	beforeStartMu *sync.RWMutex
	started       bool
}

type subscribe struct {
	eventName string
	callback  func([]byte) error
	options   geb.OnEventOptions
}

type publish struct {
	eventName string
	payload   []byte
	done      chan error
}

type Option func(q *Handler)

func Timeout(duration time.Duration) Option {
	return func(q *Handler) {
		q.timeout = duration
	}
}

func VHost(vhost string) Option {
	return func(q *Handler) {
		q.vhost = vhost
	}
}

func NewHandler(consumerName string, userName string, password string, host string, port int, options ...Option) *Handler {
	h := &Handler{
		consumerName: consumerName,
		username:     userName,
		password:     password,
		host:         host,
		port:         port,
		timeout:      time.Second * 5,

		publishCh:  make(chan *publish, maxPendingPublishes),
		subscribes: make(map[string]*subscribe),
		onError:    func(err error) {},

		connectionMu:     new(sync.RWMutex),
		shutdownCh:       make(chan struct{}),
		connectionClosed: new(uint32),
		shutdownWg:       new(sync.WaitGroup),

		beforeStartMu: new(sync.RWMutex),
	}

	for _, option := range options {
		option(h)
	}

	return h
}

func (h *Handler) Start() error {
	h.beforeStartMu.Lock()
	defer h.beforeStartMu.Unlock()

	if h.started {
		return errors.New("already started")
	}
	h.started = true

	err := h.connect()
	if err != nil {
		return err
	}

	h.shutdownWg.Add(1)
	go h.reconnect()

	return nil
}

func (h *Handler) Close() error {
	// stop reconnecting
	close(h.shutdownCh)
	h.shutdownWg.Wait()

	// do not send error from amqp closing notification goroutines
	atomic.StoreUint32(h.connectionClosed, 1)

	h.disconnect()

	return nil
}

func (h *Handler) OnEvent(eventName string, callback func([]byte) error, options geb.OnEventOptions) error {
	h.beforeStartMu.RLock()
	defer h.beforeStartMu.RUnlock()
	if h.started {
		return errors.New("OnEvent handlers must be registered before calling Start")
	}

	h.subscribes[eventName] = &subscribe{
		eventName: eventName,
		callback:  callback,
		options:   options,
	}

	return nil
}

func (h *Handler) OnError(cb func(err error)) error {
	h.beforeStartMu.RLock()
	defer h.beforeStartMu.RUnlock()
	if h.started {
		return errors.New("OnError handler must be registered before calling Start")
	}

	h.onError = cb

	return nil
}

func (h *Handler) Publish(eventName string, payload []byte) error {
	h.beforeStartMu.RLock()
	defer h.beforeStartMu.RUnlock()

	if !h.started {
		return errors.New("Publish must be called after Start")
	}

	done := make(chan error, 1)
	h.publishCh <- &publish{
		eventName: eventName,
		payload:   payload,
		done:      done,
	}

	return <-done
}

func (h *Handler) publishLoop(wg *sync.WaitGroup) {
	defer wg.Done()

	var msgCounter uint64 = 1
	pendingPublishes := make(map[uint64]chan error)

	for {
		select {
		case pub := <-h.publishCh:
			err := h.amqpPublishCh.Publish(exchangeName, pub.eventName, false, false, amqp.Publishing{
				Body: pub.payload,
			})
			if err != nil {
				pub.done <- err
				continue
			}

			done := pub.done
			pendingPublishes[msgCounter] = done
			msgCounter++
		case conf, ok := <-h.publishConfirm:
			if !ok {
				// probably we will reconnecting, so disable this chan
				h.publishConfirm = nil
				continue
			}

			done := pendingPublishes[conf.DeliveryTag]
			if !conf.Ack {
				done <- errors.New("Publish failed: ack false")
			} else {
				done <- nil
			}

			delete(pendingPublishes, conf.DeliveryTag)
		case <-h.publishCloseCh:
			err := errors.New("connection was closed during publishing")
			for _, done := range pendingPublishes {
				done <- err
			}

			return
		}
	}
}

func (h *Handler) reconnect() {
	defer h.shutdownWg.Done()

	t := time.NewTicker(time.Second)

	for {
		select {
		case <-t.C:
			if atomic.LoadUint32(h.connectionClosed) == 0 {
				continue
			}

			for {
				h.disconnect()

				err := h.connect()
				if err == nil {
					break
				}

				h.onError(err)
				time.Sleep(time.Second)
			}
		case <-h.shutdownCh:
			return
		}

	}
}

func (h *Handler) connect() error {
	h.connectionMu.Lock()
	defer h.connectionMu.Unlock()

	h.disconnectWg = new(sync.WaitGroup)
	h.consumeWg = new(sync.WaitGroup)
	h.publishWg = new(sync.WaitGroup)
	h.disconnectCh = make(chan struct{})
	h.publishCloseCh = make(chan struct{})
	h.sharedCloseCh = make(chan error)
	h.consumeChannels = make([]*amqp.Channel, 0, len(h.subscribes))

	err := h.createConnection()
	if err != nil {
		return err
	}

	err = h.createPublishChannel()
	if err != nil {
		return err
	}
	h.publishWg.Add(1)
	go h.publishLoop(h.publishWg)

	for eventName, sub := range h.subscribes {
		ch, deliveries, err := h.createConsumeChannel(eventName, h.consumerName, sub)
		if err != nil {
			return err
		}
		h.consumeChannels = append(h.consumeChannels, ch)

		h.consumeWg.Add(1)
		go sub.consume(deliveries, h.consumeWg)
	}

	atomic.StoreUint32(h.connectionClosed, 0)

	h.disconnectWg.Add(1)
	go h.handleAmqpClose()

	return nil
}

func (h *Handler) disconnect() {
	h.connectionMu.Lock()
	defer h.connectionMu.Unlock()

	if h.connection == nil {
		return
	}

	for _, ch := range h.consumeChannels {
		_ = ch.Close()
	}
	h.consumeWg.Wait()

	close(h.publishCloseCh)
	h.publishWg.Wait()

	close(h.disconnectCh)
	h.disconnectWg.Wait()

	_ = h.connection.Close()
	h.connection = nil
}

func (h *Handler) createConnection() error {
	conn, err := amqp.DialConfig(fmt.Sprintf("amqp://%v:%v@%v:%v/%v", h.username, h.password, h.host, h.port, h.vhost),
		amqp.Config{
			Heartbeat: h.timeout / 2,
			Locale:    "en_US",
			Dial:      h.dial,
		})
	if err != nil {
		return errors.Wrap(err, "Couldn't connect to rabbitmq server")
	}
	h.connection = conn

	connCloseCh := make(chan *amqp.Error)
	h.connection.NotifyClose(connCloseCh)
	h.disconnectWg.Add(1)
	go h.proxyClose("connection", connCloseCh)

	return nil
}

func (h *Handler) createPublishChannel() error {
	pubCh, err := h.connection.Channel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publishMessage channel")
	}
	h.amqpPublishCh = pubCh

	pubCloseCh := make(chan *amqp.Error)
	h.amqpPublishCh.NotifyClose(pubCloseCh)
	h.disconnectWg.Add(1)
	go h.proxyClose("publish channel", pubCloseCh)

	h.publishConfirm = h.amqpPublishCh.NotifyPublish(make(chan amqp.Confirmation, maxPendingPublishes+1))

	err = h.amqpPublishCh.Confirm(false)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (h *Handler) createConsumeChannel(eventName string, consumerName string, sub *subscribe) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := h.connection.Channel()
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create rabbitmq channel")
	}

	consumeCloseCh := make(chan *amqp.Error)
	ch.NotifyClose(consumeCloseCh)
	h.disconnectWg.Add(1)
	go h.proxyClose("consume channel", consumeCloseCh)

	err = ch.Qos(sub.options.MaxGoroutines+extraPrefetch, 0, false)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create rabbitmq channel")
	}

	queueName := fmt.Sprintf("%v/%v", consumerName, eventName)
	_, err = ch.QueueDeclare(queueName, true, false, false, false, amqp.Table{"x-queue-type": "quorum"})
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't create queue")
	}

	err = ch.QueueBind(queueName, eventName, exchangeName, false, nil)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't bind queue")
	}

	deliveries, err := ch.Consume(
		queueName,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return nil, nil, errors.Wrap(err, "couldn't start consuming")
	}

	return ch, deliveries, nil
}

// every channel and connection must have a separate close channel and a separate goroutine, because the amqp library close the closeCh
// and can't close a chan multiple times, because it will panic
func (h *Handler) proxyClose(source string, closeCh chan *amqp.Error) {
	defer h.disconnectWg.Done()

	var err error

	select {
	case err = <-closeCh:
		if err != nil {
			err = errors.WithMessage(err, "closing "+source)
		} else {
			err = errors.New("closing " + source)
		}
	case <-h.disconnectCh:
		return
	}

	select {
	case h.sharedCloseCh <- err:
	default:
	}
}

func (h *Handler) handleAmqpClose() {
	defer h.disconnectWg.Done()

	var err error
	select {
	case err = <-h.sharedCloseCh:
		if !atomic.CompareAndSwapUint32(h.connectionClosed, 0, 1) {
			return
		}

		h.onError(err)
	case <-h.disconnectCh:
	}
}

func (h *Handler) dial(network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, h.timeout)
	if err != nil {
		return nil, err
	}

	// Heartbeating hasn't started yet, don't stall forever on a dead server.
	// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
	// the deadline is cleared in openComplete.
	if err := conn.SetDeadline(time.Now().Add(h.timeout)); err != nil {
		return nil, err
	}

	return conn, nil
}

func (s *subscribe) consume(deliveries <-chan amqp.Delivery, wg *sync.WaitGroup) {
	defer wg.Done()

	// see: http://jmoiron.net/blog/limiting-concurrency-in-go/
	sem := make(chan bool, s.options.MaxGoroutines)

	for d := range deliveries {
		// if sem is full, concurency limit is reached
		sem <- true
		go func(d amqp.Delivery) {
			defer func() { <-sem }()

			err := s.callback(d.Body)

			if err != nil {
				_ = d.Nack(false, !d.Redelivered)
			} else {
				_ = d.Ack(false)
			}
		}(d)
	}

	// once we can fill the whole sem here, it means all gorutines have finished
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
}
