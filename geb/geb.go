package geb

import "context"

// Handler is an interface for the actual messaging implementation.
type Handler interface {
	// Start must be called after OnError, but before any Publish/OnEvent calls are made.
	Start()
	// Close can be called to gracefully close the queue. No new events will be processed,
	// but existing event processings will continue. Publishing on a closed handler will return with an error.
	Close() error
	// OnError callback is called when a non-event specific (not a marshaling/unmarshaling) error occurs.
	// eg: connection error
	OnError(cb func(err error, reconnect func())) error
	// OnEvent for msgpack and json codecs, either 'codec' or 'json' tags may be used
	OnEvent(eventName string, callback func(payload []byte) error, options OnEventOptions) error
	// Publish for msgpack and json codecs, either 'codec' or 'json' tags may be used
	Publish(eventName string, payload []byte) error
}

// Queue is the main struct for the geb client. You can publish/listen to multiple event types on a single queue.
type Queue struct {
	handler            Handler
	codec              Codec
	publishMiddlewares []Middleware
	onEventMiddlewares []Middleware
}

func NewQueue(handler Handler, codec Codec) *Queue {
	return &Queue{
		handler: handler,
		codec:   codec,
	}
}

// Start must be called after OnError, but before any Publish/OnEvent calls are made.
func (q *Queue) Start() {
	q.handler.Start()
}

// OnError callback is called when a non-event specific (not a marshaling/unmarshaling) error occurs.
// eg: connection error
func (q *Queue) OnError(callback func(err error, reconnect func())) error {
	return q.handler.OnError(callback)
}

// Close can be called to gracefully close the queue. No new events will be processed,
// but existing event processings will continue. Publishing on a closed queue will return with an error.
func (q *Queue) Close() error {
	return q.handler.Close()
}

// UsePublish adds a middleware for all Publish calls. To add a middleware to a specific Publish only, call
// Publish.Use() instead. Also see: geb.Middleware.
func (q *Queue) UsePublish(m Middleware) *Queue {
	q.publishMiddlewares = append(q.publishMiddlewares, m)

	return q
}

// UseOnEvent adds a middleware for all OnEvent processings. To add a middleware to a specific OnEvent only, call
// OnEvent.Use() instead. Also see: geb.Middleware.
func (q *Queue) UseOnEvent(m Middleware) *Queue {
	q.onEventMiddlewares = append(q.onEventMiddlewares, m)

	return q
}

// Event is the public representation of a geb event, used for both Publish and OnEvent.
type Event struct {
	eventName string
	codecEvent
	ctx context.Context
}

// EventName returns the name of the event, for debugging purposes.
func (e *Event) EventName() string {
	return e.eventName
}

// Context returns the event's context. Publish and OnEvent does NOT use this context,
// meaning that context cancel does NOT work. Middlewares should communicate through the context.
func (e *Event) Context() context.Context {
	return e.ctx
}

// SetContext can be used for setting the event's context. Publish and OnEvent does NOT use this context,
// meaning that context cancel does NOT work. Middlewares should communicate through the context.
func (e *Event) SetContext(ctx context.Context) {
	e.ctx = ctx
}
