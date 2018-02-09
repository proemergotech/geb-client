package geb

type Queue interface {
	Close() error
	OnError(callback func(err error))
	// OnEvent for msgpack and json codecs, either 'codec' or 'json' tags may be used
	OnEvent(codec Codec, eventName string, callback func(Event) error)
	// Publish for msgpack and json codecs, either 'codec' or 'json' tags may be used
	Publish(codec Codec, eventName string, headers map[string]string, body interface{}) (err error)
	Reconnect()
}

type Event interface {
	Headers() map[string]string
	Unmarshal(v interface{}) error
}
