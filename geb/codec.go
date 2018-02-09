package geb

import (
	"github.com/pkg/errors"
	"github.com/ugorji/go/codec"
)

type Codec interface {
	Name() string
	Encode(headers map[string]string, body interface{}) ([]byte, error)
	Decode(data []byte) (Event, error)
}

type goCodec struct {
	wrapperHandle codec.Handle
	bodyHandle    codec.Handle
}

type goEvent struct {
	handle     codec.Handle
	Body       codec.Raw         `codec:"body"`
	HeadersMap map[string]string `codec:"headers"`
}

type rawCodec struct{}

type rawEvent []byte

var (
	MsgpackCodec = func() Codec {
		wrapHandle := &codec.MsgpackHandle{}
		wrapHandle.Raw = true

		return &goCodec{
			wrapperHandle: wrapHandle,
			bodyHandle:    &codec.MsgpackHandle{},
		}
	}()
	JSONCodec = func() Codec {
		wrapHandle := &codec.JsonHandle{}
		wrapHandle.Raw = true
		wrapHandle.MapKeyAsString = true
		wrapHandle.HTMLCharsAsIs = true

		bodyHandle := &codec.JsonHandle{}
		bodyHandle.MapKeyAsString = true
		bodyHandle.HTMLCharsAsIs = true

		return &goCodec{
			wrapperHandle: wrapHandle,
			bodyHandle:    bodyHandle,
		}
	}()
	RawCodec Codec = &rawCodec{}
)

func (c *goCodec) Name() (string) {
	return c.wrapperHandle.Name()
}

func (c *goCodec) Encode(headers map[string]string, body interface{}) ([]byte, error) {
	b := []byte(nil)
	enc := codec.NewEncoderBytes(&b, c.bodyHandle)
	err := enc.Encode(body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	e := goEvent{
		HeadersMap: headers,
		Body:       b,
	}

	b = []byte(nil)
	enc = codec.NewEncoderBytes(&b, c.wrapperHandle)
	err = enc.Encode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return b, nil
}

func (c *goCodec) Decode(data []byte) (Event, error) {
	e := &goEvent{handle: c.bodyHandle}

	dec := codec.NewDecoderBytes(data, c.wrapperHandle)
	err := dec.Decode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return e, nil
}

func (e goEvent) Headers() map[string]string {
	return e.HeadersMap
}

func (e goEvent) Unmarshal(v interface{}) error {
	dec := codec.NewDecoderBytes(e.Body, e.handle)
	err := dec.Decode(&v)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *rawCodec) Name() (string) {
	return "raw"
}

func (*rawCodec) Encode(headers map[string]string, body interface{}) ([]byte, error) {
	if headers != nil {
		return nil, errors.New("geb.rawCodec.Encode: headers not supported in raw mode")
	}

	bodyB, ok := body.([]byte)
	if !ok {
		return nil, errors.New("geb.rawCodec.Encode: body must be []byte in raw mode")
	}

	return bodyB, nil
}

func (*rawCodec) Decode(data []byte) (Event, error) {
	return rawEvent(data), nil
}

func (rawEvent) Headers() map[string]string {
	return nil
}

func (r rawEvent) Unmarshal(v interface{}) error {
	vB, ok := v.(*[]byte)
	if !ok || vB == nil {
		return errors.New("geb.rawEvent.Unmarshal: v must be a non-nil *[]byte in stream mode")
	}

	*vB = r

	return nil
}
