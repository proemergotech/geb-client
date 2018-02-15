package geb

type Callback func(*Event) error

type Middleware func(e *Event, next func(*Event) error) error

func (mn Callback) wrapWith(m Middleware) Callback {
	return func(e *Event) error {
		nextErr := error(nil)
		nextCalled := false
		next := func(e *Event) error {
			if nextCalled {
				return nextErr
			}

			nextCalled = true
			return mn(e)
		}

		err := m(e, next)
		if err != nil {
			return err
		}

		return next(e)
	}
}
