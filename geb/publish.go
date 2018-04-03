package geb

import "context"

// Publish is the struct for preparing publishes.
type Publish struct {
	q          *Queue
	codec      Codec
	eventName  string
	middleware Callback
	ctx        context.Context
}

// Publish returns a builder for a Publish call.
// Add all the necessary middlewares, then call Do() for actually publishing the event.
func (q *Queue) Publish(eventName string) *Publish {
	p := &Publish{
		q:         q,
		codec:     q.codec,
		eventName: eventName,
		ctx:       context.Background(),
	}

	p.middleware = func(e *Event) error {
		payload, err := p.codec.Encode(e.codecEvent)
		if err != nil {
			return err
		}

		return p.q.handler.Publish(p.eventName, payload)
	}

	for _, m := range q.publishMiddlewares {
		p.Use(m)
	}

	return p
}

// Codec can be used to set the codec for the Publish. By default the codec is inherited from the queue.
func (p *Publish) Codec(codec Codec) *Publish {
	p.codec = codec
	return p
}

// Context sets the Publish context. It runs before any middlewares,
// so all middlewares will have access to this context.
func (p *Publish) Context(ctx context.Context) *Publish {
	p.ctx = ctx
	return p
}

// Use a middleware for this specific Publish. Also see: geb.Middleware.
func (p *Publish) Use(m Middleware) *Publish {
	p.middleware = p.middleware.wrapWith(m)
	return p
}

// Headers sets the event's headers.
func (p *Publish) Headers(headers map[string]string) *Publish {
	return p.Use(func(e *Event, next func(*Event) error) error {
		e.SetHeaders(headers)

		return next(e)
	})
}

// Body sets the event's body.
func (p *Publish) Body(v interface{}) *Publish {
	return p.Use(func(e *Event, next func(*Event) error) error {
		err := e.Marshal(v)
		if err != nil {
			return err
		}

		return next(e)
	})
}

// Do publishes the event, using the configuration and middlewares stored in the Publish object.
// All middlewares MUST be registered before calling Do.
func (p *Publish) Do() error {
	e := &Event{
		eventName:  p.eventName,
		codecEvent: p.codec.NewEvent(),
		ctx:        p.ctx,
	}

	return p.middleware(e)
}
