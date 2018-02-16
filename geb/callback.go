package geb

type Callback func(*Event) error

type Middleware func(e *Event, next func(*Event) error) error

func (c Callback) wrapWith(m Middleware) Callback {
	return func(e *Event) error {
		return m(e, c)
	}
}
