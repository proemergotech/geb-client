package geb

import (
    "github.com/streadway/amqp"
    "fmt"
    "gitlab.com/proemergotech/re-error/reerror"
    "sync"
    "log"
    "github.com/pkg/errors"
    "sync/atomic"
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

    isClosing     uint32

    onError  func(error *amqp.Error)
    handlers map[string]*handler
}

type handler struct {
    isConnected bool
    callback    func(message []byte) (error)
}

type publishMessage struct {
    eventName string
    message   []byte
    confirm   chan amqp.Confirmation
}

const exchangeName = "events"
const maxPendingPublishes = 100

func NewQueue(consumerName string, userName string, password string, host string, port int) *Queue {

    return &Queue{
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
    }
}

func (q *Queue) Publish(eventName string, message []byte) (err error) {
    rr := reerror.New(&err)
    defer rr.Recover()

    publishCh, err := q.createPublishChannel()
    rr.Panic("Couldn't create publish channel")

    pubMes := &publishMessage{
        eventName: eventName,
        message:   message,
        confirm:   make(chan amqp.Confirmation),
    }

    publishCh <- pubMes

    confirm := <-pubMes.confirm
    if !confirm.Ack {
        err = errors.New("Publish failed")
        return
    }

    return
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

            //log.Printf("publishcreate once")
            rr := reerror.New(&q.publishChErr)
            defer rr.Recover()

            //log.Printf("creating publishMessage channel")
            var conn *amqp.Connection
            conn, q.publishChErr = q.connect()
            rr.Panic("")

            var pubCh *amqp.Channel
            pubCh, q.publishChErr = conn.Channel()
            rr.Panic("Couldn't create publishMessage channel")

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

func (q *Queue) OnEvent(eventName string, callback func(message []byte) (error)) {
    q.handlers[eventName] = &handler{
        isConnected: false,
        callback:    callback,
    }

    q.startConsume()
}

func (q *Queue) startConsume() {
    var err error
    rr := reerror.New(&err)
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
    defer rr.Recover()

    q.consumeChsMutex.Lock()
    defer q.consumeChsMutex.Unlock()

    for eventName, handler := range q.handlers {
        if handler.isConnected {
            continue
        }

        var deliveries <-chan amqp.Delivery
        deliveries, err = q.createConsumeChannel(eventName)
        rr.Panic("Couldn't create channel")

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

            rr := reerror.New(&q.connectionErr)
            defer rr.Recover()

            conn, q.connectionErr = amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:%v/", q.userName, q.password, q.host, q.port))
            rr.Panic("Couldn't connect to rabbitmq server")

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
    rr := reerror.New(&err)
    defer rr.Recover()

    conn, err := q.connect();
    rr.Panic("")

    ch, err := conn.Channel()
    rr.Panic("Couldn't create rabbitmq channel")

    queueName := fmt.Sprintf("%v/%v", q.consumerName, eventName)
    _, err = ch.QueueDeclare(queueName, true, false, false, false, nil)
    rr.Panic("Couldn't create queue")

    err = ch.QueueBind(queueName, eventName, exchangeName, false, nil)
    rr.Panic("Couldn't bind queue")

    deliveries, err = ch.Consume(
        queueName,
        "",
        false,
        false,
        false,
        false,
        nil,
    )
    rr.Panic("Couldn't start consuming")

    log.Printf("queue bound: %v", queueName)

    return
}

func (q *Queue) handle(deliveries <-chan amqp.Delivery, callback func(message []byte) (error)) {
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
