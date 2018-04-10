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

type Handler struct {
	consumerName string
	userName     string
	password     string
	host         string
	port         int

	connM   sync.Mutex
	connErr error
	conn    *amqp.Connection

	publishChM sync.RWMutex
	publishCh  chan *publishMessage

	subscriberChsM sync.Mutex
	subscribers    map[string]*subscriber

	isClosing uint32
	onError   func(err error)

	timeout time.Duration
}

type subscriber struct {
	consuming bool
	callback  func([]byte) error
	options   geb.OnEventOptions
}

type publishMessage struct {
	eventName string
	message   []byte
	wg        sync.WaitGroup
	err       error
}

type Option func(q *Handler)

const exchangeName = "events"
const maxPendingPublishes = 100

// NewHandler creates a rabbitmq based geb handler. Every eventName for OnEvent will result
// in a separate rabbitmq queue. Registering multiple OnEvent callbacks for a single eventName is not supported.
func NewHandler(consumerName string, userName string, password string, host string, port int, options ...Option) *Handler {
	h := &Handler{
		consumerName: consumerName,
		userName:     userName,
		password:     password,
		host:         host,
		port:         port,

		connM:   sync.Mutex{},
		connErr: nil,
		conn:    nil,

		publishChM: sync.RWMutex{},
		publishCh:  nil,

		subscriberChsM: sync.Mutex{},
		subscribers:    make(map[string]*subscriber),

		isClosing: 0,
		onError:   func(err error) {},

		timeout: time.Second * 5,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

func (h *Handler) Publish(eventName string, payload []byte) error {
	err := h.initPublishChannel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publish channel")
	}

	h.publishChM.RLock()
	if h.publishCh == nil {
		h.publishChM.RUnlock()
		return errors.New("Connection closed while publishing")
	}

	pubMes := newPublishMessage(eventName, payload)

	h.publishCh <- pubMes
	h.publishChM.RUnlock()

	err = pubMes.Wait()
	if err != nil {
		return err
	}

	return nil
}

func (h *Handler) initPublishChannel() error {
	h.publishChM.RLock()
	publishCh := h.publishCh
	h.publishChM.RUnlock()

	if publishCh != nil {
		return nil
	}

	h.publishChM.Lock()
	defer h.publishChM.Unlock()

	if h.publishCh != nil {
		return nil
	}

	h.connM.Lock()
	defer h.connM.Unlock()

	err := h.connect()
	if err != nil {
		return err
	}

	var pubCh *amqp.Channel
	pubCh, err = h.conn.Channel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publishMessage channel")
	}

	pendingPubMsgs := make(chan *publishMessage, maxPendingPublishes)
	publishConfirm := pubCh.NotifyPublish(make(chan amqp.Confirmation, maxPendingPublishes+1))
	go func() {
		for conf := range publishConfirm {
			pubMsg := <-pendingPubMsgs
			if !conf.Ack {
				pubMsg.Done(errors.New("Publish failed: ack false"))
			} else {
				pubMsg.Done(nil)
			}
		}
		for pubMsg := range pendingPubMsgs {
			pubMsg.Done(errors.New("Connection was closed during publishing"))
		}
	}()

	publishCh = make(chan *publishMessage)
	go func() {
		for pubMsg := range publishCh {
			err := pubCh.Publish(exchangeName, pubMsg.eventName, false, false, amqp.Publishing{
				Body: pubMsg.message,
			})
			if err != nil {
				pubMsg.Done(err)
			} else {
				pendingPubMsgs <- pubMsg
			}
		}
		pubCh.Close()
		close(pendingPubMsgs)
	}()

	err = pubCh.Confirm(false)
	if err != nil {
		close(publishCh)

		return err
	}

	h.publishCh = publishCh

	return nil
}

func (h *Handler) OnEvent(eventName string, callback func([]byte) error, options geb.OnEventOptions) {
	h.subscriberChsM.Lock()
	h.subscribers[eventName] = &subscriber{
		consuming: false,
		callback:  callback,
		options:   options,
	}
	h.subscriberChsM.Unlock()

	h.startConsume()
}

func (h *Handler) startConsume() {
	h.subscriberChsM.Lock()
	defer h.subscriberChsM.Unlock()

	for eventName, sub := range h.subscribers {
		if sub.consuming {
			continue
		}

		deliveries, err := h.createConsumeChannel(eventName, sub.options)
		if err != nil {
			return
		}

		go sub.consume(deliveries)

		sub.consuming = true
	}
}

func (h *Handler) OnError(callback func(err error)) {
	h.onError = callback
}

func (h *Handler) connect() error {
	if h.conn != nil {
		return nil
	}

	if h.connErr != nil {
		return h.connErr
	}

	h.conn, h.connErr = amqp.DialConfig(fmt.Sprintf("amqp://%v:%v@%v:%v/", h.userName, h.password, h.host, h.port),
		amqp.Config{
			Heartbeat: h.timeout / 2,
			Locale:    "en_US",
			Dial:      h.dial,
		})
	if h.connErr != nil {
		h.connErr = errors.Wrap(h.connErr, "Couldn't connect to rabbitmq server")
		h.onError(&amqp.Error{
			Code:    -1,
			Reason:  fmt.Sprintf("%v", h.connErr),
			Server:  false,
			Recover: true,
		})

		return h.connErr
	}

	closeCh := make(chan *amqp.Error)
	h.conn.NotifyClose(closeCh)
	h.handleClose(closeCh)

	return nil
}

func (h *Handler) createConsumeChannel(eventName string, options geb.OnEventOptions) (<-chan amqp.Delivery, error) {
	h.connM.Lock()
	defer h.connM.Unlock()

	err := h.connect()
	if err != nil {
		return nil, err
	}

	defer func() {
		if err != nil {
			h.onError(&amqp.Error{
				Code:    -1,
				Reason:  fmt.Sprintf("%v", err),
				Server:  false,
				Recover: true,
			})
		}
	}()

	ch, err := h.conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create rabbitmq channel")
	}

	err = ch.Qos(options.MaxGoroutines+extraPrefetch, 0, false)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create rabbitmq channel")
	}

	queueName := fmt.Sprintf("%v/%v", h.consumerName, eventName)
	_, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create queue")
	}

	err = ch.QueueBind(queueName, eventName, exchangeName, false, nil)
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't bind queue")
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
		return nil, errors.Wrap(err, "Couldn't start consuming")
	}

	return deliveries, nil
}

func (s *subscriber) consume(deliveries <-chan amqp.Delivery) {
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

func (h *Handler) Close() (err error) {
	atomic.StoreUint32(&h.isClosing, 1)
	defer func() {
		atomic.StoreUint32(&h.isClosing, 0)
	}()

	h.subscriberChsM.Lock()
	defer h.subscriberChsM.Unlock()

	h.publishChM.Lock()
	defer h.publishChM.Unlock()

	h.connM.Lock()
	defer h.connM.Unlock()

	for _, sub := range h.subscribers {
		sub.consuming = false
	}

	if h.publishCh != nil {
		close(h.publishCh)
		h.publishCh = nil
	}

	if h.conn != nil {
		err = h.conn.Close()
		h.conn = nil
	}

	return err
}

func (h *Handler) Reconnect() {
	h.connM.Lock()
	h.connErr = nil
	h.connM.Unlock()

	h.startConsume()
}

func (h *Handler) handleClose(errors <-chan *amqp.Error) {
	go func() {
		err := <-errors

		if atomic.LoadUint32(&h.isClosing) != 1 {
			h.Close()
			if err != nil && h.onError != nil {
				h.onError(err)
			}
		}

		for range errors {
		}
	}()
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

func Timeout(duration time.Duration) Option {
	return func(q *Handler) {
		q.timeout = duration
	}
}

func newPublishMessage(eventName string, payload []byte) *publishMessage {
	pubMes := &publishMessage{
		eventName: eventName,
		message:   payload,
		wg:        sync.WaitGroup{},
		err:       nil,
	}

	pubMes.wg.Add(1)

	return pubMes
}

func (pm *publishMessage) Done(err error) {
	pm.err = err
	pm.wg.Done()
}

func (pm *publishMessage) Wait() error {
	pm.wg.Wait()
	return pm.err
}
