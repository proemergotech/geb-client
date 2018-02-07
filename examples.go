package main

import (
	"time"
	"log"
	"sync"
	"sync/atomic"
	"os"
	"os/signal"
	"syscall"
	"gitlab.com/proemergotech/geb-client-go/geb/rabbitmq"
	"gitlab.com/proemergotech/geb-client-go/geb"
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

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		done <- true
	}()

	go func() {
		wg.Wait()
		log.Println("All published")
	}()

	<-done
	log.Printf("publishCount :%v consumeCount: %v\n", publishCount, consumeCount)
}

func createQueue() geb.Queue {
	return rabbitmq.NewQueue(
		"goTest",
		"service",
		"service",
		"10.20.3.8",
		5672,
		rabbitmq.Timeout(5*time.Second),
	)
}

func publish(queue geb.Queue, eventName string) {
	err := queue.Publish(eventName, []byte("test message"))
	if err != nil {
		time.Sleep(1 * time.Second)
		log.Printf("error: %v\n", err)
		return
	}
	log.Printf("event: %s published\n", eventName)
	atomic.AddUint64(&publishCount, 1)
}

func consume(queue geb.Queue, eventName string) {
	queue.OnError(func(error error) {
		log.Printf("connection error %#v\n", error)

		go func() {
			time.Sleep(2 * time.Second)
			queue.Reconnect()
		}()
	})

	queue.OnEvent(eventName, func(message []byte) (err error) {
		//log.Printf("here %v", string(message))
		atomic.AddUint64(&consumeCount, 1)
		log.Printf("event: %s consumed\n", eventName)

		return
	})
}
