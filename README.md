# Geb Client in Go

## Installation

1. Use an ssh agent to allow git clone without any user input
2. Add dependency to go.mod (see below)
3. run go build

### go.mod

```vgo
    github.com/proemergotech/geb-client v2.0.0
```

## Usage

```go
	queue := geb.NewQueue(
		rabbitmq.NewHandler(
			"goTest",    // consumerName (application name)
			"service",   // rabbitmq username
			"service",   // rabbitmq password
			"10.20.3.8", // rabbitmq host
			5672,        // rabbitmq port
			rabbitmq.Timeout(5*time.Second),
		),
		geb.JSONCodec(),
	)
	queue.OnError(func(err error) {
		log.Printf("You broke it! %+v", errors.WithStack(err))
	})
	err := queue.Start()
    if err != nil {
        panic(err)    
    }   
	defer queue.Close()

	type dragon struct {
		Color string `json:"color" mycustomtag:"color,omitempty"` // default tag names are "json" or "codec"
	}

	queue.OnEvent("event/dragon/created/v1").
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

	d := dragon{
		Color: "green",
	}
	err := queue.Publish("event/dragon/created/v1").
		Headers(map[string]string{"x_dragon_heads": "3"}).
		Body(d).
		Do()

	if err != nil {
		log.Printf("You broke it! %+v", errors.WithStack(err))
	}

	time.Sleep(2 * time.Second)
```

for more details see [examples.go](examples.go)
