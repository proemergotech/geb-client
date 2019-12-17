package geb

import (
	"time"

	"github.com/pkg/errors"
)

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
			if pArg := recover(); pArg != nil {
				if pErr, ok := pArg.(error); ok {
					err = errors.Wrap(pErr, "panic recovered in geb recovery middleware")
				} else {
					err = errors.Errorf("panic recovered in geb recovery middleware: %v", pArg)
				}
			}
		}()
		return next(e)
	}
}

func RetryMiddleware() Middleware {
	return func(e *Event, next func(*Event) error) error {
		err := next(e)
		if err == nil {
			return nil
		}

		backOff := NewExponentialBackOff(DefaultMaxElapsedTime, DefaultMaxInterval, DefaultRandomizationFactor)
		for {
			hasNext, duration := backOff.NextBackOff()
			if !hasNext {
				return errors.Wrap(err, "retry error")
			}
			time.Sleep(duration)
			if err = next(e); err == nil {
				return nil
			}
		}
	}
}
