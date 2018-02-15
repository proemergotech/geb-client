package geb

import "context"

type Publish struct {
	q          *Queue
	codec      Codec
	eventName  string
	middleware Callback
}

func (q *Queue) Publish(eventName string) *Publish {
	p := &Publish{
		q:         q,
		codec:     q.codec,
		eventName: eventName,
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

func (p *Publish) Codec(codec Codec) *Publish {
	p.codec = codec
	return p
}

func (p *Publish) Use(m Middleware) *Publish {
	p.middleware = p.middleware.wrapWith(m)
	return p
}

func (p *Publish) Headers(headers map[string]string) *Publish {
	return p.Use(func(e *Event, next func(*Event) error) error {
		e.SetHeaders(headers)

		return next(e)
	})
}

func (p *Publish) Body(v interface{}) *Publish {
	return p.Use(func(e *Event, next func(*Event) error) error {
		err := e.Marshal(v)
		if err != nil {
			return err
		}

		return next(e)
	})
}

func (p *Publish) Do() error {
	e := &Event{
		eventName:  p.eventName,
		codecEvent: p.codec.NewEvent(),
		ctx:        context.Background(),
	}

	return p.middleware(e)
}
