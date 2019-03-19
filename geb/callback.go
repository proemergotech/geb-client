package geb

import "github.com/pkg/errors"

// Callback is signature for the callback method passed to OnEvent.
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

func RecoveryMiddleware() Middleware {
	return func(e *Event, next func(*Event) error) (err error) {
		defer func() {
			if panicErr := recover(); panicErr != nil {
				if pErr, ok := panicErr.(error); ok {
					err = errors.Wrap(pErr, "panic recovered in geb recovery middleware")
				} else {
					err = errors.New("panic recovered in geb recovery middleware")
				}
			}
		}()
		return next(e)
	}
}
