package geb

// Callback is signature for the callback method passed to OnEvent. Also used internally to represent the outer layer
// of the middleware stack.
type Callback func(*Event) error

// Middleware is the signature of middlewares.
// Middlewares are layered (like an onion), meaning that the last added middleware will be executed first.
// You should always call next() from the middleware. If next() is not called,
// none of the inner (previously added) middlewares will run.
type Middleware func(e *Event, next func(*Event) error) error

func (c Callback) wrapWith(m Middleware) Callback {
	return func(e *Event) error {
		return m(e, c)
	}
}
