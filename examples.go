package main

import (
	"encoding/json"
	"flag"
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
	Field1 string         `json:"field_1" custom:"field_1"`
	Field2 map[int]string `json:"field_2" custom:"field_2"`
	Field3 []float64      `json:"field_3" custom:"field_3"`
	bodyEmbedded
}

type bodyEmbedded struct {
	Field4 string `json:"field_4" custom:"field_4"`
}

var tests = []test{
	{
		codec:     geb.JSONCodec(),
		eventName: "goTest/json/v1",
		headers: map[string]string{
			"header": "value",
		},
		body: testBody,
	},
	{
		codec:     geb.JSONCodec(geb.UseTag("custom")),
		eventName: "goTest/msgpack/v1",
		headers: map[string]string{
			"header": "value",
		},
		body: testBody,
	},
	{
		codec:     geb.RawCodec(),
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

var (
	publishCounts    = make(map[string]*uint64, len(tests))
	publishErrCounts = make(map[string]*uint64, len(tests))
	consumeCounts    = make(map[string]*uint64, len(tests))
	midConsCounts    = make(map[string]*uint64, len(tests))
	serverHost       string
	serverPort       int
	serverUser       string
	serverPass       string
)

func simple() {
	queue := geb.NewQueue(
		rabbitmq.NewHandler(
			"goTest",   // consumerName (application name)
			serverUser, // rabbitmq username
			serverPass, // rabbitmq password
			serverHost, // rabbitmq host
			serverPort, // rabbitmq port
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)

	defer queue.Close()

	type dragon struct {
		Color string `json:"color" mycustomtag:"color,omitempty"` // default tag names are "json" or "codec"
	}

	queue.OnEvent("event/dragon/created/v1").
		Listen(func(event *geb.Event) error {
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
	err := queue.Publish("event/dragon/created/v1").
		Headers(map[string]string{"x_dragon_heads": "3"}).
		Body(d).
		Do()

	if err != nil {
		log.Printf("You broke it! %+v", err)
	}

	time.Sleep(2 * time.Second)
}

func main() {
	flag.StringVar(&serverHost, "host", "10.20.3.8", "Geb server host")
	flag.IntVar(&serverPort, "port", 5672, "Geb server port")
	flag.StringVar(&serverUser, "user", "service", "Geb server user name")
	flag.StringVar(&serverPass, "pass", "service", "Geb server password")
	flag.Parse()

	//simple()
	//return

	var wg sync.WaitGroup
	start := make(chan bool)
	done := make(chan bool)

	for _, t := range tests {
		consumeCounts[t.eventName] = new(uint64)
		midConsCounts[t.eventName] = new(uint64)
		publishCounts[t.eventName] = new(uint64)
		publishErrCounts[t.eventName] = new(uint64)
	}
	defer func() {
		wg.Wait()

		pc, _ := json.Marshal(publishCounts)
		pec, _ := json.Marshal(publishErrCounts)
		cc, _ := json.Marshal(consumeCounts)
		mc, _ := json.Marshal(midConsCounts)

		log.Printf("publishCounts :%v\n", string(pc))
		log.Printf("publishErrCounts :%v\n", string(pec))
		log.Printf("consumeCounts :%v\n", string(cc))
		log.Printf("midConsCounts :%v\n", string(mc))
	}()

	publishQ := createQueue()
	defer publishQ.Close()

	until := time.Now().Add(10 * time.Second)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		t := tests[i%len(tests)]
		go func(q *geb.Queue, t test) {
			<-start
			defer wg.Done()
			for time.Now().Before(until) {
				select {
				case <-done:
					return
				default:
				}

				publish(q, t)
			}
		}(publishQ, t)
	}

	for i := 0; i < 2; i++ {
		var consumeQ *geb.Queue
		if i == 0 {
			consumeQ = publishQ
		} else {
			consumeQ = createQueue()
			defer consumeQ.Close()
		}

		consumeQ.UseOnEvent(func(e *geb.Event, next func(e *geb.Event) error) error {
			atomic.AddUint64(midConsCounts[e.EventName()], 1)

			return next(e)
		})

		for _, t := range tests {
			go func(q *geb.Queue, t test) {
				<-start
				consume(q, t)
			}(consumeQ, t)
		}
	}

	close(start)

	sigs := make(chan os.Signal, 1)

	signal.Notify(sigs, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		<-sigs
		close(done)
	}()

	go func() {
		wg.Wait()
		log.Println("All published")
	}()

	<-done
}

func createQueue() *geb.Queue {
	q := geb.NewQueue(
		rabbitmq.NewHandler(
			"goTest",
			serverUser,
			serverPass,
			serverHost,
			serverPort,
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)

	q.OnError(func(error error) {
		log.Printf("connection error %+v\n", error)

		go func() {
			time.Sleep(2 * time.Second)
			q.Reconnect()
		}()
	})

	return q
}

func publish(queue *geb.Queue, t test) {
	err := queue.Publish(t.eventName).
		Codec(t.codec).
		Headers(t.headers).
		Body(t.body).
		Do()

	if err != nil {
		count := atomic.AddUint64(publishErrCounts[t.eventName], 1)
		if count%1000 == 0 {
			log.Printf("event: %v publish error %v times\n", t.eventName, count)
		}
		// log.Printf("event publish err: %+v", err)
		return
	}
	count := atomic.AddUint64(publishCounts[t.eventName], 1)
	if count%1000 == 0 {
		log.Printf("event: %v published %v times\n", t.eventName, count)
	}
	//log.Printf("event: %s published\n", t.eventName)
}

func consume(queue *geb.Queue, t test) {
	queue.OnEvent(t.eventName, geb.MaxGoroutines(1000)).
		Codec(t.codec).
		Listen(func(event *geb.Event) error {
			body2Ptr := reflect.New(reflect.ValueOf(t.body).Type())

			event.Unmarshal(body2Ptr.Interface())

			//log.Printf("%#+v", t.eventName)
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

			return nil
		})
}
