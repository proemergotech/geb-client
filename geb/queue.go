package geb

import (
    "github.com/streadway/amqp"
    "fmt"
    "gitlab.com/proemergotech/re-error/reerror"
    "sync"
    "log"
    "github.com/pkg/errors"
)

type Queue struct {
    consumerName string
    userName     string
    password     string
    host         string
    port         int

    connectInit sync.Once
    connection  *amqp.Connection

    publishInit       sync.Once
    publishChannel    *amqp.Channel
    publishCh         chan *publishMessage
    publishConfirmers chan chan amqp.Confirmation

    isClosing bool

    onError  func(error *amqp.Error)
    handlers map[string]func(message []byte) (error)
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

        connectInit: sync.Once{},
        connection:  nil,

        publishInit:       sync.Once{},
        publishChannel:    nil,
        publishCh:         nil,
        publishConfirmers: nil,

        isClosing: false,

        onError:  nil,
        handlers: make(map[string]func(message []byte) (error)),
    }
}

func (q *Queue) Publish(eventName string, message []byte) (err error) {
    rr := reerror.New(&err)
    defer rr.Recover()

    //log.Printf("here1")
    err = q.createPublishChannel()
    //log.Printf("here2")
    rr.Panic("Couldn't create publish channel")

    pubMes := &publishMessage{
        eventName: eventName,
        message:   message,
        confirm:   make(chan amqp.Confirmation),
    }

    //log.Printf("here3")
    q.publishCh <- pubMes
    //log.Printf("here4")

    confirm := <-pubMes.confirm
    //log.Printf("here5")
    if !confirm.Ack {
        err = errors.New("Publish failed")
        return
    }

    return
}

func (q *Queue) createPublishChannel() (err error) {
    q.publishInit.Do(func() {
        rr := reerror.New(&err)
        defer rr.Recover()

        //log.Printf("creating publishMessage channel")
        var conn *amqp.Connection
        conn, err = q.connect()
        rr.Panic("")

        var pubCh *amqp.Channel
        pubCh, err = conn.Channel()
        rr.Panic("Couldn't create publishMessage channel")

        q.publishConfirmers = make(chan chan amqp.Confirmation, maxPendingPublishes)
        publishConfirm := pubCh.NotifyPublish(make(chan amqp.Confirmation))
        go func() {
            for confirmer := range q.publishConfirmers {
                confirmer <- <-publishConfirm
            }
        }()

        q.publishCh = make(chan *publishMessage)
        go func() {
            for pubMes := range q.publishCh {
                q.publishConfirmers <- pubMes.confirm

                pubCh.Publish(exchangeName, pubMes.eventName, false, false, amqp.Publishing{
                    Body: pubMes.message,
                })
            }
        }()

        pubCh.Confirm(false)
        q.publishChannel = pubCh
    })

    return
}

func (q *Queue) OnEvent(eventName string, callback func(message []byte) (error)) (err error) {
    rr := reerror.New(&err)
    defer rr.Recover()

    deliveries, err := q.createConsumeChannel(eventName)
    rr.Panic("Couldn't create channel")

    q.handlers[eventName] = callback
    go q.handle(deliveries, callback)

    return
}

func (q *Queue) OnError(callback func(error *amqp.Error)) {
    q.onError = callback
}

func (q *Queue) connect() (conn *amqp.Connection, err error) {
    rr := reerror.New(&err)
    defer rr.Recover()

    if q.connection == nil {
        conn, err = amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:%v/", q.userName, q.password, q.host, q.port))
        rr.Panic("Couldn't connect to rabbitmq server")

        errrorsCh := make(chan *amqp.Error)
        conn.NotifyClose(errrorsCh)
        q.handleErrors(errrorsCh)

        q.connection = conn
    }

    conn = q.connection

    return
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
    q.isClosing = true
    defer func() {
        q.isClosing = false
    }()

    q.connectInit.Do(func() {})
    q.publishInit.Do(func() {})

    if q.publishConfirmers != nil {
        close(q.publishConfirmers)
        q.publishConfirmers = nil
    }

    if q.publishCh != nil {
        close(q.publishCh)
        q.publishCh = nil
    }

    if q.publishChannel != nil {
        // connection close will close channel as well
        q.publishChannel = nil
    }

    if q.connection != nil {
        err = q.connection.Close()
        q.connection = nil
    }

    q.connectInit = sync.Once{}
    q.publishInit = sync.Once{}

    return
}

func (q *Queue) Reconnect() {
    q.Close()

    for eventName, callback := range q.handlers {
        q.OnEvent(eventName, callback)
    }
}

func (q *Queue) handleErrors(errors <-chan *amqp.Error) {
    go func() {
        for err := range errors {
            if !q.isClosing && q.onError != nil {
                q.onError(err)
            }
        }
    }()
}
