package rabbitmq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gitlab.com/proemergotech/geb-client-go/geb"
)

const extraPrefetch = 20
const exchangeName = "events"
const maxPendingPublishes = 100

type Handler struct {
	consumerName string
	userName     string
	password     string
	host         string
	port         int
	timeout      time.Duration

	conn   *connection
	events *events
	wg     *sync.WaitGroup

	subscribes map[string]*subscribe
	onError    func(err error, reconnect func())

	closed       *uint32
	beforeStartM *sync.RWMutex
	started      bool
}

type events struct {
	connect    chan struct{}
	disconnect chan error
	publish    chan *publish
}

type Option func(q *Handler)

type connection struct {
	conn           *amqp.Connection
	pubCh          *amqp.Channel
	pendingPubMsgs chan *publish
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

// NewHandler creates a rabbitmq based geb handler. Every eventName for OnEvent will result
// in a separate rabbitmq queue. Registering multiple OnEvent callbacks for a single eventName is not supported.
func NewHandler(consumerName string, userName string, password string, host string, port int, options ...Option) *Handler {
	h := &Handler{
		consumerName: consumerName,
		userName:     userName,
		password:     password,
		host:         host,
		port:         port,
		timeout:      time.Second * 5,

		events: &events{
			connect:    make(chan struct{}),
			disconnect: make(chan error),
			publish:    make(chan *publish, 5),
		},
		wg: &sync.WaitGroup{},

		subscribes: make(map[string]*subscribe),

		onError: func(err error, reconnect func()) {},
		closed:  new(uint32),

		beforeStartM: &sync.RWMutex{},
	}

	for _, option := range options {
		option(h)
	}

	return h
}

func (h *Handler) Start() {
	h.beforeStartM.Lock()
	h.started = true
	h.beforeStartM.Unlock()

	h.wg.Add(1)
	go h.loop()

	h.events.connect <- struct{}{}
}

func (h *Handler) OnEvent(eventName string, callback func([]byte) error, options geb.OnEventOptions) error {
	h.beforeStartM.RLock()
	defer h.beforeStartM.RUnlock()
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

func (h *Handler) OnError(cb func(err error, reconnect func())) error {
	h.beforeStartM.RLock()
	defer h.beforeStartM.RUnlock()
	if h.started {
		return errors.New("OnError handler must be registered before calling Start")
	}

	h.onError = cb

	return nil
}

func (h *Handler) Publish(eventName string, payload []byte) error {
	h.beforeStartM.RLock()
	defer h.beforeStartM.RUnlock()

	if !h.started {
		return errors.New("Publish must be called after Start")
	}

	done := make(chan error, 1)
	h.events.publish <- &publish{
		eventName: eventName,
		payload:   payload,
		done:      done,
	}

	return <-done
}

func (h *Handler) Reconnect() {
	h.events.disconnect <- nil
	h.events.connect <- struct{}{}
}

func (h *Handler) Close() error {
	atomic.StoreUint32(h.closed, 1)

	h.beforeStartM.RLock()
	defer h.beforeStartM.RUnlock()
	if !h.started {
		return nil
	}

	h.events.disconnect <- nil
	h.wg.Wait()

	return nil
}

func Timeout(duration time.Duration) Option {
	return func(q *Handler) {
		q.timeout = duration
	}
}

func (h *Handler) loop() {
	defer h.wg.Done()
	defer h.drain()
	// h.closed can be set to true before disconnect is handled
	defer h.disconnect()

	var err error
	for atomic.LoadUint32(h.closed) == 0 {
		select {
		case <-h.events.connect:
			if h.conn != nil {
				continue
			}

			err = h.connect()
			if err != nil {
				h.conn = nil
				h.onError(err, h.Reconnect)
				continue
			}

			for eventName, sub := range h.subscribes {
				err = h.createConsumeChannel(eventName, sub)
				if err != nil {
					h.conn = nil
					h.onError(err, h.Reconnect)
					break
				}
			}

		case err = <-h.events.disconnect:
			h.disconnect()
			if err != nil {
				h.onError(err, h.Reconnect)
			}

		case pub := <-h.events.publish:
			if h.conn == nil {
				pub.done <- errors.New("publishing on closed amqp connnection")
				continue
			}

			err = h.conn.pubCh.Publish(exchangeName, pub.eventName, false, false, amqp.Publishing{
				Body: pub.payload,
			})
			if err != nil {
				pub.done <- err
				continue
			}

			select {
			case h.conn.pendingPubMsgs <- pub:
				// publish done will be called after ack

			case err = <-h.events.disconnect:
				h.disconnect()
				if err != nil {
					h.onError(err, h.Reconnect)
					pub.done <- errors.Wrap(err, "connection closed during publish")
				} else {
					pub.done <- errors.New("connection closed during publish")
				}
			}
		}
	}
}

func (h *Handler) drain() {
	go func() {
		for range h.events.connect {
		}
	}()
	go func() {
		for range h.events.disconnect {
		}
	}()
	go func() {
		for p := range h.events.publish {
			p.done <- errors.New("publish called after close")
		}
	}()
}

func (h *Handler) disconnect() {
	if h.conn == nil {
		return
	}

	h.conn.conn.Close()
	close(h.conn.pendingPubMsgs)

	h.conn = nil
}

func (h *Handler) connect() error {
	conn, err := amqp.DialConfig(fmt.Sprintf("amqp://%v:%v@%v:%v/", h.userName, h.password, h.host, h.port),
		amqp.Config{
			Heartbeat: h.timeout / 2,
			Locale:    "en_US",
			Dial:      h.dial,
		})
	if err != nil {
		return errors.Wrap(err, "Couldn't connect to rabbitmq server")
	}

	closeCh := make(chan *amqp.Error)
	conn.NotifyClose(closeCh)
	go h.handleAmqpClose(closeCh)

	pubCh, err := conn.Channel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publishMessage channel")
	}

	publishConfirm := pubCh.NotifyPublish(make(chan amqp.Confirmation, maxPendingPublishes+1))
	pendingPubMsgs := make(chan *publish, maxPendingPublishes)
	go h.handlePublishResults(publishConfirm, pendingPubMsgs)

	err = pubCh.Confirm(false)
	if err != nil {
		return errors.WithStack(err)
	}

	h.conn = &connection{
		conn:           conn,
		pubCh:          pubCh,
		pendingPubMsgs: pendingPubMsgs,
	}

	return nil
}

func (h *Handler) handleAmqpClose(closeCh chan *amqp.Error) {
	aErr := <-closeCh
	if aErr != nil {
		h.events.disconnect <- aErr
	}
}

func (h *Handler) handlePublishResults(publishConfirm chan amqp.Confirmation, pendingPubMsgs chan *publish) {
	// publishConfirm is guaranteed to be in the same order as pendingPubMsgs,
	// but either one might be sorter (closed sooner) than the other
	for conf := range publishConfirm {
		pubMsg, ok := <-pendingPubMsgs
		if !ok {
			// disconnect, there might be some publishes that could not be sent to pendingPublishes, but they were already returned an error
			for range publishConfirm {
				// drain the remaining confirms (this channel should be closed already by disconnect)
			}
			return
		}

		if !conf.Ack {
			pubMsg.done <- errors.New("Publish failed: ack false")
		} else {
			pubMsg.done <- nil
		}
	}
	for pubMsg := range pendingPubMsgs {
		pubMsg.done <- errors.New("connection was closed during publishing")
	}
}

func (h *Handler) createConsumeChannel(eventName string, sub *subscribe) error {
	ch, err := h.conn.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "couldn't create rabbitmq channel")
	}

	err = ch.Qos(sub.options.MaxGoroutines+extraPrefetch, 0, false)
	if err != nil {
		return errors.Wrap(err, "couldn't create rabbitmq channel")
	}

	queueName := fmt.Sprintf("%v/%v", h.consumerName, eventName)
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return errors.Wrap(err, "couldn't create queue")
	}

	err = ch.QueueBind(queueName, eventName, exchangeName, false, nil)
	if err != nil {
		return errors.Wrap(err, "couldn't bind queue")
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
		return errors.Wrap(err, "couldn't start consuming")
	}

	go sub.consume(deliveries)

	return nil
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

func (s *subscribe) consume(deliveries <-chan amqp.Delivery) {
	// see: http://jmoiron.net/blog/limiting-concurrency-in-go/
	sem := make(chan bool, s.options.MaxGoroutines)

	for d := range deliveries {
		// if sem is full, concurency limit is reached
		sem <- true
		go func(d amqp.Delivery) {
			defer func() { <-sem }()

			err := s.callback(d.Body)

			if err != nil {
				d.Nack(false, !d.Redelivered)
			} else {
				d.Ack(false)
			}
		}(d)
	}

	// once we can fill the whole sem here, it means all gorutines have finished
	for i := 0; i < cap(sem); i++ {
		sem <- true
	}
}
