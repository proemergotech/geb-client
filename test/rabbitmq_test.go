package test

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/pkg/errors"
	"github.com/streadway/amqp"
	"gitlab.com/proemergotech/geb-client-go/geb"
	"gitlab.com/proemergotech/geb-client-go/geb/rabbitmq"
)

type body struct {
	ID uint32 `json:"id"`
}

var (
	testHeaders = map[string]string{"header": "value"}

	serverHost    = os.Getenv("GEB_HOST")
	serverPort, _ = strconv.Atoi(os.Getenv("GEB_PORT"))
	serverUser    = os.Getenv("GEB_USERNAME")
	serverPass    = os.Getenv("GEB_PASSWORD")

	serviceName = "goTest"
	queueCount  = 3

	publishRoutines        = 50
	publishCountPerRoutine = 100
	expectedPublishCount   = publishRoutines * publishCountPerRoutine
)

func TestNoError(t *testing.T) {
	p := NewProxy(serverHost, serverPort)
	counts := NewCounter(expectedPublishCount)

	p.Start()
	test(t, p, counts)

	if counts.TriedPublishes() != counts.PublishCount() {
		t.Errorf("Expected no publish errors, got: %v", counts.TriedPublishes()-counts.PublishCount())
	}
}

func TestDelayedJoin(t *testing.T) {
	p := NewProxy(serverHost, serverPort)
	counts := NewCounter(expectedPublishCount)

	go func() {
		time.Sleep(1 * time.Second)
		p.Start()
	}()
	test(t, p, counts)

	if counts.PublishCount() == 0 {
		t.Errorf("Expected some publish success, got none")
	}

	if counts.TriedPublishes() == counts.PublishCount() {
		t.Errorf("Expected some publish errors, got none")
	}

	if counts.ConsumeCount() == 0 {
		t.Errorf("Expected some consume success, got none")
	}
}

func TestSeverRestart(t *testing.T) {
	p := NewProxy(serverHost, serverPort)
	counts := NewCounter(expectedPublishCount)
	var publishBeforeRestart int

	p.Start()

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()

		time.Sleep(1 * time.Second)
		p.Stop()
		time.Sleep(1 * time.Second)
		publishBeforeRestart = counts.PublishCount()
		p.Start()
	}()

	test(t, p, counts)

	wg.Wait()

	if counts.PublishCount() == 0 {
		t.Errorf("Expected some publish success, got none")
	}

	if publishBeforeRestart == 0 {
		t.Errorf("Expected some publish success before proxy stop, got none")
	}

	if counts.PublishCount() == publishBeforeRestart {
		t.Errorf("Expected some publish success after proxy restart, got none")
	}

	if counts.TriedPublishes() == counts.PublishCount() {
		t.Errorf("Expected some publish errors, got none")
	}

	if counts.ConsumeCount() == 0 {
		t.Errorf("Expected some consume success, got none")
	}
}

func TestOnEventAfterStart(t *testing.T) {
	p := NewProxy(serverHost, serverPort)
	p.Start()

	q := geb.NewQueue(
		rabbitmq.NewHandler(
			serviceName,
			serverUser,
			serverPass,
			p.Host(),
			p.Port(),
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)

	q.Start()

	err := q.OnEvent("", geb.MaxGoroutines(1000)).
		Listen(func(event *geb.Event) error {
			return nil
		})
	if err == nil {
		t.Fatal("expected error when registering OnEvent callback after Start, got none")
	}
}

func TestOnErrorAfterStart(t *testing.T) {
	p := NewProxy(serverHost, serverPort)
	p.Start()

	q := geb.NewQueue(
		rabbitmq.NewHandler(
			serviceName,
			serverUser,
			serverPass,
			p.Host(),
			p.Port(),
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)

	q.Start()

	err := q.OnError(func(err error, reconnect func()) {
	})
	if err == nil {
		t.Fatal("expected error when registering OnEvent callback after Start, got none")
	}
}

func test(t *testing.T, p *Proxy, counts *Counter) {
	eventName := "event/goTest/test/" + t.Name() + "/v1"
	queueName := serviceName + "/" + eventName
	err := deleteQueue(queueName)
	if err != nil {
		t.Fatal(errors.Wrap(err, "couldn't delete queue"))
	}

	queues := make([]*geb.Queue, 0, queueCount)
	for i := 0; i < queueCount; i++ {
		queues = append(queues, createQueue(eventName, t, p, counts))
	}
	defer func() {
		for _, q := range queues {
			err = q.Close()
			if err != nil {
				t.Error(errors.WithStack(err))
			}
		}
	}()

	counter := new(uint32)

	start := make(chan bool)
	wg := &sync.WaitGroup{}
	wg.Add(publishRoutines)
	for i := 0; i < publishRoutines; i++ {
		go func(q *geb.Queue) {
			defer wg.Done()
			<-start
			for j := 0; j < publishCountPerRoutine; j++ {
				id := atomic.AddUint32(counter, 1)
				err := q.Publish(eventName).
					Codec(geb.JSONCodec()).
					Headers(testHeaders).
					Body(&body{ID: id}).
					Do()
				if err == nil {
					counts.AddPublish(id)
				}

				avgSleep := float64(3 * time.Second / time.Duration(publishCountPerRoutine))
				time.Sleep(time.Duration(math.Floor(avgSleep * 2 * rand.Float64())))
			}
		}(queues[rand.Intn(len(queues))])
	}
	close(start)

	wg.Wait()

	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		time.Sleep(200 * time.Millisecond)

		if counts.PublishedNotConsumed() == 0 {
			break
		}
	}

	if counts.PublishedNotConsumed() != 0 {
		t.Errorf(
			"%v publishes received ack but were not consumed",
			counts.PublishedNotConsumed(),
		)
	}

	t.Log(counts.String())
}

func createQueue(eventName string, t *testing.T, p *Proxy, counts *Counter) *geb.Queue {
	q := geb.NewQueue(
		rabbitmq.NewHandler(
			serviceName,
			serverUser,
			serverPass,
			p.Host(),
			p.Port(),
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)

	err := q.OnError(func(err error, reconnect func()) {
		go func() {
			time.Sleep(2 * time.Second)
			reconnect()
		}()
	})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	err = q.OnEvent(eventName, geb.MaxGoroutines(1000)).
		Listen(func(event *geb.Event) error {
			b := &body{}
			event.Unmarshal(b)

			if !reflect.DeepEqual(testHeaders, event.Headers()) {
				t.Errorf("headers mismatch, expected: %v, got: %v", testHeaders, event.Headers())
			}

			if b.ID <= 0 {
				t.Errorf("body mismatch, ID should be positive: %v", b)
			}

			counts.AddConsume(b.ID)

			return nil
		})
	if err != nil {
		t.Fatalf("%+v", err)
	}

	q.Start()

	return q
}

func deleteQueue(queueName string) error {
	conn, err := amqp.Dial(fmt.Sprintf("amqp://%v:%v@%v:%v/", serverUser, serverPass, serverHost, serverPort))
	if err != nil {
		return errors.WithStack(err)
	}

	ch, err := conn.Channel()
	if err != nil {
		return errors.WithStack(err)
	}

	_, err = ch.QueueDelete(queueName, false, false, false)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}
