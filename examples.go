package main

import (
    "gitlab.com/proemergotech/geb-client-go/geb"
    "github.com/streadway/amqp"
    "time"
    "log"
    "sync"
    "sync/atomic"
    "os"
    "os/signal"
    "syscall"
)

var eventNames = []string{
    "goTest/created/v1",
    "goTest/created/v2",
    "goTest/created/v3",
}

var publishCount uint64 = 0
var consumeCount uint64 = 0

func main() {
    queue := createQueue()
    defer queue.Close()

    var wg sync.WaitGroup
    start := make(chan bool)

    until := time.Now().Add(30 * time.Second)
    for i := 0; i < 100; i++ {
        wg.Add(1)
        eventName := eventNames[i%len(eventNames)]
        go func() {
            <-start
            defer wg.Done()
            for time.Now().Before(until) {
                publish(queue, eventName)
                atomic.AddUint64(&publishCount, 1)
            }
        }()
    }

    for i := 0; i < 3; i++ {
        eventName := eventNames[i%len(eventNames)]
        go func() {
            <-start
            consume(queue, eventName)
        }()
    }

    close(start)

    wg.Wait()
    log.Printf("All published")

    sigs := make(chan os.Signal, 1)
    done := make(chan bool, 1)

    signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

    go func() {
        <-sigs
        done <- true
    }()

    <-done
    log.Printf("publishCount :%v consumeCount: %v", publishCount, consumeCount)
}

func createQueue() *geb.Queue {
    return geb.NewQueue("does_not_matter_for_publish", "service", "service", "192.168.188.87", 5672)
}

func publish(queue *geb.Queue, eventName string) {
    err := queue.Publish(eventName, []byte("test message"))
    if err != nil {
        log.Printf("error: %+v", err)
    }
}

func consume(queue *geb.Queue, eventName string) {
    var err error

    queue = geb.NewQueue("goTest/consume", "service", "service", "192.168.188.87", 5672)

    queue.OnError(func(error *amqp.Error) {
        time.Sleep(1 * time.Second)
        log.Printf("connection error")
        queue.Reconnect()
    })
    err = queue.OnEvent(eventName, func(message []byte) (err error) {
        //log.Printf("here %v", string(message))
        atomic.AddUint64(&consumeCount, 1)

        return
    })

    if err != nil {
        log.Printf("Couldn't create channel: %+v", err)
    }
}
