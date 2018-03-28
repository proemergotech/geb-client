package geb

import (
	"context"
)

type OnEventOption func(oe *OnEventOptions)

type errCodecEvent struct {
	err error
}

type OnEventOptions struct {
	MaxGoroutines int
}

type OnEvent struct {
	q          *Queue
	codec      Codec
	eventName  string
	callback   Callback
	middleware Callback
	options    OnEventOptions
}

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

func (oe *OnEvent) Codec(c Codec) *OnEvent {
	oe.codec = c
	return oe
}

func (oe *OnEvent) Use(m Middleware) *OnEvent {
	oe.middleware = oe.middleware.wrapWith(m)
	return oe
}

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
	return
}

func (ece *errCodecEvent) Unmarshal(v interface{}) error {
	return ece.err
}

func (ece *errCodecEvent) Marshal(v interface{}) error {
	return ece.err
}

func MaxGoroutines(maxGoroutines int) OnEventOption {
	return func(o *OnEventOptions) {
		o.MaxGoroutines = maxGoroutines
	}
}
