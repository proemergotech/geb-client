package geb

import (
	"context"
)

type OnEventOption func(oe *OnEventOptions)

type errCodecEvent struct {
	err error
}

// OnEventOptions contains the options for event subscriptions (should be supported by Handler implementations.)
// This should NOT be used directly, instead use methods with OnEventOption return type.
type OnEventOptions struct {
	MaxGoroutines int
}

// OnEvent is the struct for building OnEvent subscriptions.
type OnEvent struct {
	q          *Queue
	codec      Codec
	eventName  string
	callback   Callback
	middleware Callback
	options    OnEventOptions
}

// OnEvent returns a builder for an OnEvent subscription.
// Add all the necessary middlewares, then call Listen(callback).
func (q *Queue) OnEvent(eventName string, options ...OnEventOption) *OnEvent {
	oe := &OnEvent{
		q:         q,
		codec:     q.codec,
		eventName: eventName,
		options:   OnEventOptions{MaxGoroutines: 1},
	}

	for _, option := range options {
		option(&oe.options)
	}

	oe.middleware = func(e *Event) error {
		return oe.callback(e)
	}

	for _, m := range q.onEventMiddlewares {
		oe.Use(m)
	}

	return oe
}

// Codec can be used to set the codec for the OnEvent. By default the codec is inherited from the queue.
func (oe *OnEvent) Codec(c Codec) *OnEvent {
	oe.codec = c
	return oe
}

// Use a middleware for this specific OnEvent processing. Also see: geb.Middleware.
func (oe *OnEvent) Use(m Middleware) *OnEvent {
	oe.middleware = oe.middleware.wrapWith(m)
	return oe
}

// Listen starts the OnEvent subscription, using the configuration and middlewares stored in the OnEvent object.
// All middlewares MUST be registered before calling Listen.
func (oe *OnEvent) Listen(cb Callback) {
	oe.callback = cb

	oe.q.handler.OnEvent(oe.eventName, func(payload []byte) error {
		ce, err := oe.codec.Decode(payload)

		if err != nil {
			return oe.middleware(
				&Event{
					eventName: oe.eventName,
					codecEvent: &errCodecEvent{
						err: err,
					},
				},
			)
		}

		return oe.middleware(&Event{
			eventName:  oe.eventName,
			codecEvent: ce,
			ctx:        context.Background(),
		})
	}, oe.options)
}

func (*errCodecEvent) Headers() map[string]string {
	return map[string]string{}
}

func (*errCodecEvent) SetHeaders(map[string]string) {
}

func (ece *errCodecEvent) Unmarshal(v interface{}) error {
	return ece.err
}

func (ece *errCodecEvent) Marshal(v interface{}) error {
	return ece.err
}

// MaxGoroutines sets the maximum number of concurrent OnEvent callbacks for the given eventName.
func MaxGoroutines(maxGoroutines int) OnEventOption {
	return func(o *OnEventOptions) {
		o.MaxGoroutines = maxGoroutines
	}
}
