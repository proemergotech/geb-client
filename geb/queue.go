package geb

import (
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
)

type Queue struct {
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

	onError  func(error *amqp.Error)
	handlers map[string]*handler

	Timeout time.Duration
}

type handler struct {
	isConnected bool
	callback    func(message []byte) error
}

type publishMessage struct {
	eventName string
	message   []byte
	confirm   chan amqp.Confirmation
}

type Option func(q *Queue)

const exchangeName = "events"
const maxPendingPublishes = 100

func NewQueue(consumerName string, userName string, password string, host string, port int, options ...Option) *Queue {
	queue := &Queue{
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

		onError:  func(error *amqp.Error) {},
		handlers: make(map[string]*handler),

		Timeout: time.Second * 5,
	}

	for _, option := range options {
		option(queue)
	}

	return queue
}

func (q *Queue) Publish(eventName string, message []byte) (err error) {
	publishCh, err := q.createPublishChannel()
	if err != nil {
		return errors.Wrap(err, "Couldn't create publish channel")
	}

	pubMes := &publishMessage{
		eventName: eventName,
		message:   message,
		confirm:   make(chan amqp.Confirmation),
	}

	publishCh <- pubMes

	confirm := <-pubMes.confirm
	if !confirm.Ack {
		return errors.New("Publish failed")
	}

	return
}

func (q *Queue) PublishStruct(eventName string, message interface{}) (err error) {
	msg, err := json.Marshal(message)
	if err != nil {
		return errors.Wrapf(err, "geb.Queue.PublishStruct: json marshal failed for event: %v, with message type %T"+eventName, message)
	}

	return q.Publish(eventName, msg)
}

func (q *Queue) createPublishChannel() (publishCh chan *publishMessage, err error) {
	//log.Printf("publishcreate")

	publishCh, err = q.publishCh, q.publishChErr

	if atomic.LoadUint32(&q.publishChExists) == 0 || q.publishCh == nil {
		q.publishChMutex.Lock()
		defer q.publishChMutex.Unlock()

		defer func() {
			publishCh, err = q.publishCh, q.publishChErr
		}()

		if q.publishChExists == 0 {
			q.publishChErr = nil
			defer func() {
				if q.publishChErr != nil {
					q.closePublishChannel(false)
				} else {
					q.publishChExists = 1
				}
			}()

			//log.Printf("creating publishMessage channel")
			var conn *amqp.Connection
			conn, q.publishChErr = q.connect()
			if q.publishChErr != nil {
				return
			}

			var pubCh *amqp.Channel
			pubCh, q.publishChErr = conn.Channel()
			if q.publishChErr != nil {
				q.publishChErr = errors.Wrap(err, "Couldn't create publishMessage channel")
				return
			}

			publishConfirmers := make(chan chan amqp.Confirmation, maxPendingPublishes)
			publishConfirm := pubCh.NotifyPublish(make(chan amqp.Confirmation))
			go func() {
				for confirmer := range publishConfirmers {
					confirmer <- <-publishConfirm
				}
			}()

			q.publishCh = make(chan *publishMessage)
			go func() {
				for pubMes := range q.publishCh {
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

func (q *Queue) closePublishChannel(lock bool) {
	if lock {
		q.publishChMutex.Lock()
		defer q.publishChMutex.Unlock()
	}

	if q.publishCh != nil {
		close(q.publishCh)
		q.publishCh = nil
	}

	q.publishChExists = 0
}

func (q *Queue) OnEvent(eventName string, callback func(message []byte) error) {
	q.handlers[eventName] = &handler{
		isConnected: false,
		callback:    callback,
	}

	q.startConsume()
}

func (q *Queue) startConsume() {
	var err error
	defer func() {
		if err != nil {
			q.onError(&amqp.Error{
				Code:    -1,
				Reason:  fmt.Sprintf("%v", err),
				Server:  false,
				Recover: true,
			})
		}
	}()

	q.consumeChsMutex.Lock()
	defer q.consumeChsMutex.Unlock()

	for eventName, handler := range q.handlers {
		if handler.isConnected {
			continue
		}

		var deliveries <-chan amqp.Delivery
		deliveries, err = q.createConsumeChannel(eventName)
		if err != nil {
			err = errors.Wrap(err, "Couldn't create channel")
			return
		}

		go q.handle(deliveries, handler.callback)

		handler.isConnected = true
	}

	return
}

func (q *Queue) OnError(callback func(error *amqp.Error)) {
	q.onError = callback
}

func (q *Queue) connect() (conn *amqp.Connection, err error) {
	//log.Printf("connecting")

	conn, err = q.connection, q.connectionErr

	if atomic.LoadUint32(&q.connectionExists) == 0 || q.connection == nil {
		q.connectionMutex.Lock()
		defer q.connectionMutex.Unlock()

		defer func() {
			conn, err = q.connection, q.connectionErr
		}()

		if atomic.LoadUint32(&q.connectionExists) == 0 {
			q.connectionErr = nil
			defer func() {
				if q.connectionErr != nil {
					q.closeConnection(false)
				} else {
					q.connectionExists = 1
				}
			}()

			//log.Printf("connecting once")

			conn, q.connectionErr = amqp.DialConfig(fmt.Sprintf("amqp://%v:%v@%v:%v/", q.userName, q.password, q.host, q.port),
				amqp.Config{
					Heartbeat: q.Timeout / 2,
					Locale:    "en_US",
					Dial:      q.dial,
				})
			if q.connectionErr != nil {
				q.connectionErr = errors.Wrap(err, "Couldn't connect to rabbitmq server")
				return
			}

			closeCh := make(chan *amqp.Error)
			conn.NotifyClose(closeCh)
			q.handleClose(closeCh)

			q.connection = conn
		}
	}

	return
}

func (q *Queue) closeConnection(lock bool) {
	if lock {
		q.connectionMutex.Lock()
		defer q.connectionMutex.Unlock()
	}

	if q.connection != nil {
		q.connection.Close()
		q.connection = nil
	}

	q.connectionExists = 0
}

func (q *Queue) createConsumeChannel(eventName string) (deliveries <-chan amqp.Delivery, err error) {
	conn, err := q.connect()
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("Couldn't create rabbitmq channel")
	}

	ch, err := conn.Channel()
	if err != nil {
		return nil, errors.Wrap(err, "Couldn't create rabbitmq channel")
	}

	queueName := fmt.Sprintf("%v/%v", q.consumerName, eventName)
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

func (q *Queue) handle(deliveries <-chan amqp.Delivery, callback func(message []byte) error) {
	//log.Printf("starting handling: %#v", deliveries)
	for d := range deliveries {
		//log.Printf("delivery: %v", d)
		err := callback(d.Body)
		if err != nil {
			d.Nack(false, !d.Redelivered)
		} else {
			d.Ack(false)
		}
	}
}

func (q *Queue) Close() (err error) {
	//log.Printf("closing")

	atomic.StoreUint32(&q.isClosing, 1)
	defer func() {
		atomic.StoreUint32(&q.isClosing, 0)
	}()

	q.consumeChsMutex.Lock()
	for _, handler := range q.handlers {
		handler.isConnected = false
	}
	q.consumeChsMutex.Unlock()

	q.closeConnection(true)
	q.closePublishChannel(true)

	//log.Printf("closing end")
	return
}

func (q *Queue) Reconnect() {
	//log.Printf("reconnecting")

	q.startConsume()
}

func (q *Queue) handleClose(errors <-chan *amqp.Error) {
	go func() {
		for err := range errors {
			if atomic.LoadUint32(&q.isClosing) == 1 {
				continue
			}

			q.Close()

			if q.onError != nil {
				q.onError(err)
			}
		}
	}()
}

func (q *Queue) dial(network, addr string) (net.Conn, error) {
	conn, err := net.DialTimeout(network, addr, q.Timeout)
	if err != nil {
		return nil, err
	}

	// Heartbeating hasn't started yet, don't stall forever on a dead server.
	// A deadline is set for TLS and AMQP handshaking. After AMQP is established,
	// the deadline is cleared in openComplete.
	if err := conn.SetDeadline(time.Now().Add(q.Timeout)); err != nil {
		return nil, err
	}

	return conn, nil
}

func Timeout(duration time.Duration) Option {
	return func(q *Queue) {
		q.Timeout = duration
	}
}
