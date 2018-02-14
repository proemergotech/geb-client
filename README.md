# Geb Client in Go

## Installation

1. Use an ssh agent to allow git clone without any user input
2. Add dependency to Gopkg.toml (see below)
3. run dep ensure

### Gopkg.toml

```toml
[[constraint]]
  name = "gitlab.com/proemergotech/geb-client-go"
  source = "git@gitlab.com:proemergotech/geb-client-go.git"
  version = "0.1.6"
```

## Usage

```go
	queue := &geb.Queue{
		Handler: rabbitmq.NewHandler(
			"goTest",    // consumerName (application name)
			"service",   // rabbitmq username
			"service",   // rabbitmq password
			"10.20.3.8", // rabbitmq host
			5672,        // rabbitmq port
			rabbitmq.Timeout(5*time.Second),
		),
		Codec: geb.MsgpackCodec(),
	}

	defer queue.Close()

	type dragon struct {
		Color string `json:"color" mycustomtag:"color,omitempty"` // default tag names are "json" or "codec"
	}

	// optionally: geb.MsgpackCodec(geb.UseTags("mycustomtag"))
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
```

for more details see [examples.go](examples.go)
