package geb

type OnEvent struct {
	q          *Queue
	codec      Codec
	eventName  string
	callback   Callback
	middleware Callback
}

func (q *Queue) OnEvent(eventName string) *OnEvent {
	oe := &OnEvent{
		q:         q,
		codec:     q.codec,
		eventName: eventName,
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
			oe.q.onError(err)

			return nil
		}

		return oe.middleware(&Event{
			eventName:  oe.eventName,
			codecEvent: ce,
			ctx:        make(map[string]interface{}),
		})
	})
}
