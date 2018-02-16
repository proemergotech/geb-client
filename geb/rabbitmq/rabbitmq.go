package rabbitmq

import (
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Handler struct {
	consumerName string
	userName     string
	password     string
	host         string
	port         int

	connectionExists uint32
	connectionMutex  sync.Mutex
	connectionErr    error
	connection       *amqp.Connection

	publishChExists uint32
	publishChMutex  sync.Mutex
	publishChErr    error
	publishCh       chan *publishMessage

	consumeChsMutex sync.Mutex

	isClosing uint32

	onError  func(err error)
	handlers map[string]*handler

	timeout time.Duration
}

type handler struct {
	isConnected bool
	callback    func([]byte) error
}

type publishMessage struct {
	eventName string
	message   []byte
	confirm   chan amqp.Confirmation
}

type Option func(q *Handler)

const exchangeName = "events"
const maxPendingPublishes = 100

func NewHandler(consumerName string, userName string, password string, host string, port int, options ...Option) *Handler {
	h := &Handler{
		consumerName: consumerName,
		userName:     userName,
		password:     password,
		host:         host,
		port:         port,

		connectionExists: 0,
		connectionMutex:  sync.Mutex{},
		connectionErr:    nil,
		connection:       nil,

		publishChExists: 0,
		publishChMutex:  sync.Mutex{},
		publishChErr:    nil,
		publishCh:       nil,

		isClosing:       0,
		consumeChsMutex: sync.Mutex{},

		onError:  func(err error) {},
		handlers: make(map[string]*handler),

		timeout: time.Second * 5,
	}

	for _, option := range options {
		option(h)
	}

	return h
}

func (h *Handler) Publish(eventName string, payload []byte) (err error) {
	publishCh, err := h.createPublishChannel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publish channel")
	}

	pubMes := &publishMessage{
		eventName: eventName,
		message:   payload,
		confirm:   make(chan amqp.Confirmation),
	}

	publishCh <- pubMes

	confirm := <-pubMes.confirm
	if !confirm.Ack {
		return errors.New("Publish failed")
	}

	return
}

func (h *Handler) createPublishChannel() (publishCh chan *publishMessage, err error) {
	//log.Printf("publishcreate")

	publishCh, err = h.publishCh, h.publishChErr

	if atomic.LoadUint32(&h.publishChExists) == 0 || h.publishCh == nil {
		h.publishChMutex.Lock()
		defer h.publishChMutex.Unlock()

		defer func() {
			publishCh, err = h.publishCh, h.publishChErr
		}()

		if h.publishChExists == 0 {
			h.publishChErr = nil
			defer func() {
				if h.publishChErr != nil {
					h.closePublishChannel(false)
				} else {
					h.publishChExists = 1
				}
			}()

			//log.Printf("creating publishMessage channel")
			var conn *amqp.Connection
			conn, h.publishChErr = h.connect()
			if h.publishChErr != nil {
				return
			}

			var pubCh *amqp.Channel
			pubCh, h.publishChErr = conn.Channel()
			if h.publishChErr != nil {
				h.publishChErr = errors.Wrap(err, "Couldn't create publishMessage channel")
				return
			}

			publishConfirmers := make(chan chan amqp.Confirmation, maxPendingPublishes)
			publishConfirm := pubCh.NotifyPublish(make(chan amqp.Confirmation))
			go func() {
				for confirmer := range publishConfirmers {
					confirmer <- <-publishConfirm
				}
			}()

			h.publishCh = make(chan *publishMessage)
			go func() {
				for pubMes := range h.publishCh {
					publishConfirmers <- pubMes.confirm

					pubCh.Publish(exchangeName, pubMes.eventName, false, false, amqp.Publishing{
						Body: pubMes.message,
					})
				}
			}()

			pubCh.Confirm(false)
		}
	}

	return
}

func (h *Handler) closePublishChannel(lock bool) {
	if lock {
		h.publishChMutex.Lock()
		defer h.publishChMutex.Unlock()
	}

	if h.publishCh != nil {
		close(h.publishCh)
		h.publishCh = nil
	}

	h.publishChExists = 0
}

func (h *Handler) OnEvent(eventName string, callback func([]byte) error) {
	h.consumeChsMutex.Lock()
	h.handlers[eventName] = &handler{
		isConnected: false,
		callback:    callback,
	}
	h.consumeChsMutex.Unlock()

	h.startConsume()
}

func (h *Handler) startConsume() {
	var err error
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

	h.consumeChsMutex.Lock()
	defer h.consumeChsMutex.Unlock()

	for eventName, handler := range h.handlers {
		if handler.isConnected {
			continue
		}

		var deliveries <-chan amqp.Delivery
		deliveries, err = h.createConsumeChannel(eventName)
		if err != nil {
			err = errors.Wrap(err, "Couldn't create channel")
			return
		}

		go h.handle(deliveries, handler)

		handler.isConnected = true
	}

	return
}

func (h *Handler) OnError(callback func(err error)) {
	h.onError = callback
}

func (h *Handler) connect() (*amqp.Connection, error) {
	//log.Printf("connecting")

	if atomic.LoadUint32(&h.connectionExists) == 0 || h.connection == nil {
		h.connectionMutex.Lock()
		defer h.connectionMutex.Unlock()

		if atomic.LoadUint32(&h.connectionExists) == 0 {
			//log.Printf("connecting once")

			h.connection, h.connectionErr = amqp.DialConfig(fmt.Sprintf("amqp://%v:%v@%v:%v/", h.userName, h.password, h.host, h.port),
				amqp.Config{
					Heartbeat: h.timeout / 2,
					Locale:    "en_US",
					Dial:      h.dial,
				})
			if h.connectionErr != nil {
				h.connectionErr = errors.Wrap(h.connectionErr, "Couldn't connect to rabbitmq server")
				h.closeConnection(false)
				return h.connection, h.connectionErr
			}

			h.connectionExists = 1

			closeCh := make(chan *amqp.Error)
			h.connection.NotifyClose(closeCh)
			h.handleClose(closeCh)
		}
	}

	return h.connection, h.connectionErr
}

func (h *Handler) closeConnection(lock bool) {
	if lock {
		h.connectionMutex.Lock()
		defer h.connectionMutex.Unlock()
	}

	if h.connection != nil {
		h.connection.Close()
		h.connection = nil
	}

	h.connectionExists = 0
}

func (h *Handler) createConsumeChannel(eventName string) (deliveries <-chan amqp.Delivery, err error) {
	conn, err := h.connect()
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()
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

	deliveries, err = ch.Consume(
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

	return
}

func (h *Handler) handle(deliveries <-chan amqp.Delivery, handler *handler) {
	//log.Printf("starting handling: %#v", deliveries)
	for d := range deliveries {
		//log.Printf("delivery: %v", d)
		err := handler.callback(d.Body)
		if err != nil {
			d.Nack(false, !d.Redelivered)
		} else {
			d.Ack(false)
		}
	}
}

func (h *Handler) Close() (err error) {
	//log.Printf("closing")

	atomic.StoreUint32(&h.isClosing, 1)
	defer func() {
		atomic.StoreUint32(&h.isClosing, 0)
	}()

	h.consumeChsMutex.Lock()
	for _, handler := range h.handlers {
		handler.isConnected = false
	}
	h.consumeChsMutex.Unlock()

	h.closeConnection(true)
	h.closePublishChannel(true)

	//log.Printf("closing end")
	return
}

func (h *Handler) Reconnect() {
	//log.Printf("reconnecting")

	h.startConsume()
}

func (h *Handler) handleClose(errors <-chan *amqp.Error) {
	go func() {
		for err := range errors {
			if atomic.LoadUint32(&h.isClosing) == 1 {
				continue
			}

			h.Close()

			if h.onError != nil {
				h.onError(err)
			}
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
