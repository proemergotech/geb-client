package main

import (
	"flag"
	"log"
	"time"

	"github.com/pkg/errors"
	"github.com/proemergotech/geb-client-go/v2/geb"
	"github.com/proemergotech/geb-client-go/v2/geb/rabbitmq"
)

func main() {
	var (
		serverHost string
		serverPort int
		serverUser string
		serverPass string
	)

	flag.StringVar(&serverHost, "host", "10.20.3.8", "Geb server host")
	flag.IntVar(&serverPort, "port", 25672, "Geb server port")
	flag.StringVar(&serverUser, "user", "service", "Geb server user name")
	flag.StringVar(&serverPass, "pass", "service", "Geb server password")
	flag.Parse()

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
	err := queue.OnError(func(err error) {
		log.Printf("You broke it! %+v", errors.WithStack(err))
	})
	if err != nil {
		log.Panicf("You broke it! %+v", errors.WithStack(err))
	}
	defer queue.Close()

	type dragon struct {
		Color string `json:"color" mycustomtag:"color,omitempty"` // default tag names are "json" or "codec"
	}

	err = queue.OnEvent("event/dragon/created/v1").
		Listen(func(event *geb.Event) error {
			d := dragon{}
			err := event.Unmarshal(&d)
			if err != nil {
				log.Printf("You broke it! %+v", errors.WithStack(err))
				return nil
			}

			log.Printf("A mighty %v dragon with %v heads has been created!", d.Color, event.Headers()["x_dragon_heads"])
			return nil
		})
	if err != nil {
		log.Panicf("You broke it! %+v", errors.WithStack(err))
	}
	err = queue.Start()
	if err != nil {
		log.Panicf("You broke it! %+v", errors.WithStack(err))
	}

	d := dragon{
		Color: "green",
	}
	err = queue.Publish("event/dragon/created/v1").
		Headers(map[string]string{"x_dragon_heads": "3"}).
		Body(d).
		Do()

	if err != nil {
		log.Printf("You broke it! %+v", errors.WithStack(err))
	}

	time.Sleep(2 * time.Second)
}
