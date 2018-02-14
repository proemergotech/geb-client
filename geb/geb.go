package geb

import (
	"context"
)

type Handler interface {
	Close() error
	OnError(callback func(err error))
	// OnEvent for msgpack and json codecs, either 'codec' or 'json' tags may be used
	OnEvent(eventName string, callback func(payload []byte) error)
	// Publish for msgpack and json codecs, either 'codec' or 'json' tags may be used
	Publish(eventName string, payload []byte) (err error)
	Reconnect()
}

type Middleware func(e *Event, next func(*Event) error) error

type Queue struct {
	Handler            Handler
	Codec              Codec
	onError            func(err error)
	publishMiddlewares []Middleware
	onEventMiddlewares []Middleware
}

func (q *Queue) OnError(callback func(err error)) {
	q.onError = callback
	q.Handler.OnError(callback)
}

func (q *Queue) Close() error {
	return q.Handler.Close()
}

func (q *Queue) Reconnect() {
	q.Handler.Reconnect()
}

func (q *Queue) UsePublish(m Middleware) *Queue {
	q.publishMiddlewares = append(q.publishMiddlewares, m)

	return q
}

func (q *Queue) UseOnEvent(m Middleware) *Queue {
	q.onEventMiddlewares = append(q.onEventMiddlewares, m)

	return q
}

type Event struct {
	eventName string
	codecEvent
	ctx context.Context
}

func (e *Event) EventName() string {
	return e.eventName
}

func (e *Event) Context() context.Context {
	return e.ctx
}

func (e *Event) SetContext(ctx context.Context) {
	e.ctx = ctx
}
