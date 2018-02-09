package main

import (
	"encoding/json"
	"log"
	"os"
	"os/signal"
	"reflect"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"gitlab.com/proemergotech/geb-client-go/geb"
	"gitlab.com/proemergotech/geb-client-go/geb/rabbitmq"
)

type test struct {
	codec     geb.Codec
	eventName string
	headers   map[string]string
	body      interface{}
}

type body struct {
	Field1 string         `json:"field_1"`
	Field2 map[int]string `json:"field_2"`
	Field3 []float64      `json:"field_3"`
	bodyEmbedded
}

type bodyEmbedded struct {
	Field4 string `json:"field_4"`
}

var tests = []test{
	{
		codec:     geb.JSONCodec,
		eventName: "goTest/json/v1",
		headers: map[string]string{
			"header": "value",
		},
		body: testBody,
	},
	{
		codec:     geb.MsgpackCodec,
		eventName: "goTest/msgpack/v1",
		headers: map[string]string{
			"header": "value",
		},
		body: testBody,
	},
	{
		codec:     geb.RawCodec,
		eventName: "goTest/raw/v1",
		headers:   nil,
		body:      []byte("raw test"),
	},
}

var testBody = body{
	Field1: "f1ß \t@&",
	Field2: map[int]string{
		1: "test1",
		2: "test2",
	},
	Field3: []float64{
		1.45,
		-5.66,
	},
	bodyEmbedded: bodyEmbedded{
		Field4: "<br>",
	},
}

var publishCounts = make(map[string]*uint64, len(tests))
var consumeCounts = make(map[string]*uint64, len(tests))

func simple() {
	var queue geb.Queue = rabbitmq.NewQueue(
		"goTest",    // consumerName (application name)
		"service",   // rabbitmq username
		"service",   // rabbitmq password
		"10.20.3.8", // rabbitmq host
		5672,        // rabbitmq port
		rabbitmq.Timeout(5*time.Second),
	)
	defer queue.Close()

	type dragon struct {
		Color string `json:"field1"` // json or codec tag can be used
	}

	queue.OnEvent(geb.MsgpackCodec, "event/dragon/created/v1", func(event geb.Event) error {
		d := dragon{}
		err := event.Unmarshal(&d)
		if err != nil {
			log.Printf("You broke it! %+v", err)
			return nil
		}

		log.Printf("A mighty %v dragon with %v heads has been created!", d.Color, event.Headers()["x_dragon_heads"])
		return nil
	})

	d := dragon{
		Color: "green",
	}
	err := queue.Publish(geb.MsgpackCodec, "event/dragon/created/v1", map[string]string{"x_dragon_heads": "3"}, d)
	if err != nil {
		log.Printf("You broke it! %+v", err)
	}

	time.Sleep(2 * time.Second)
}

func main() {
	//simple()
	//return

	queue := createQueue()
	defer queue.Close()

	var wg sync.WaitGroup
	start := make(chan bool)

	until := time.Now().Add(10 * time.Second)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		t := tests[i%len(tests)]
		publishCounts[t.eventName] = new(uint64)
		go func() {
			<-start
			defer wg.Done()
			for time.Now().Before(until) {
				publish(queue, t)
			}
		}()
	}

	for i := 0; i < 3; i++ {
		t := tests[i%len(tests)]
		consumeCounts[t.eventName] = new(uint64)
		go func() {
			<-start
			consume(queue, t)
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

	pc, _ := json.Marshal(publishCounts)
	cc, _ := json.Marshal(consumeCounts)

	log.Printf("publishCounts :%v\n", string(pc))
	log.Printf("consumeCounts :%v\n", string(cc))
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

func publish(queue geb.Queue, t test) {
	err := queue.Publish(t.codec, t.eventName, t.headers, t.body)
	if err != nil {
		time.Sleep(1 * time.Second)
		log.Printf("error: %v\n", err)
		return
	}
	count := atomic.AddUint64(publishCounts[t.eventName], 1)
	if count%1000 == 0 {
		log.Printf("event: %v published %v times\n", t.eventName, count)
	}
	//log.Printf("event: %s published\n", t.eventName)
}

func consume(queue geb.Queue, t test) {
	queue.OnError(func(error error) {
		log.Printf("connection error %+v\n", error)

		go func() {
			time.Sleep(2 * time.Second)
			queue.Reconnect()
		}()
	})

	queue.OnEvent(t.codec, t.eventName, func(event geb.Event) (err error) {
		body2Ptr := reflect.New(reflect.ValueOf(t.body).Type())

		event.Unmarshal(body2Ptr.Interface())

		//log.Printf("%#+v", t.headers)
		//log.Printf("%#+v", event.Headers())
		//log.Printf("%#+v", t.body)
		//log.Printf("%#+v", reflect.Indirect(body2Ptr).Interface())

		if !reflect.DeepEqual(t.headers, event.Headers()) {
			log.Printf("headers mismatch for %v", t.codec.Name())
		}

		if !reflect.DeepEqual(t.body, reflect.Indirect(body2Ptr).Interface()) {
			log.Printf("body mismatch for %v", t.codec.Name())
		}

		count := atomic.AddUint64(consumeCounts[t.eventName], 1)
		if count%1000 == 0 {
			log.Printf("event: %v consumed %v times\n", t.eventName, count)
		}

		return
	})
}
