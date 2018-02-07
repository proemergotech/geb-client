package geb

type Queue interface {
	Close() error
	OnError(callback func(err error))
	OnEvent(eventName string, callback func(message []byte) error)
	Publish(eventName string, message []byte) (err error)
	PublishStruct(eventName string, message interface{}) (err error)
	Reconnect()
}
