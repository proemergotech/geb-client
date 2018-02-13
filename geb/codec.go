package geb

import (
	"github.com/pkg/errors"
	ucodec "github.com/ugorji/go/codec"
)

type Codec interface {
	Name() string
	Encode(headers map[string]string, body interface{}) ([]byte, error)
	Decode(data []byte) (Event, error)
}

type uCodec struct {
	wrapperHandle ucodec.Handle
	bodyHandle    ucodec.Handle
}

type uEvent struct {
	handle     ucodec.Handle
	Body       ucodec.Raw        `codec:"body"`
	HeadersMap map[string]string `codec:"headers"`
}

type rawCodec struct{}

type rawEvent []byte

type CodecOption func(*codecSettings)

type codecSettings struct {
	tags []string
}

func MsgpackCodec(opts ...CodecOption) Codec {
	gs := &codecSettings{
		tags: []string{"json", "codec"},
	}
	for _, opt := range opts {
		opt(gs)
	}

	wrapHandle := &ucodec.MsgpackHandle{}
	wrapHandle.TypeInfos = ucodec.NewTypeInfos([]string{"codec"})
	wrapHandle.Raw = true

	bodyHandle := &ucodec.MsgpackHandle{}
	bodyHandle.TypeInfos = ucodec.NewTypeInfos(gs.tags)

	return &uCodec{
		wrapperHandle: wrapHandle,
		bodyHandle:    bodyHandle,
	}
}

func JSONCodec(opts ...CodecOption) Codec {
	gs := &codecSettings{
		tags: []string{"json", "codec"},
	}
	for _, opt := range opts {
		opt(gs)
	}

	wrapHandle := &ucodec.JsonHandle{}
	wrapHandle.TypeInfos = ucodec.NewTypeInfos([]string{"codec"})
	wrapHandle.Raw = true
	wrapHandle.MapKeyAsString = true
	wrapHandle.HTMLCharsAsIs = true

	bodyHandle := &ucodec.JsonHandle{}
	bodyHandle.TypeInfos = ucodec.NewTypeInfos(gs.tags)
	bodyHandle.MapKeyAsString = true
	bodyHandle.HTMLCharsAsIs = true

	return &uCodec{
		wrapperHandle: wrapHandle,
		bodyHandle:    bodyHandle,
	}
}

func RawCodec() Codec {
	return &rawCodec{}
}

func UseTags(tags ...string) CodecOption {
	return func(gs *codecSettings) {
		gs.tags = tags
	}
}

func (c *uCodec) Name() string {
	return c.wrapperHandle.Name()
}

func (c *uCodec) Encode(headers map[string]string, body interface{}) ([]byte, error) {
	b := []byte(nil)
	enc := ucodec.NewEncoderBytes(&b, c.bodyHandle)
	err := enc.Encode(body)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	e := uEvent{
		HeadersMap: headers,
		Body:       b,
	}

	b = []byte(nil)
	enc = ucodec.NewEncoderBytes(&b, c.wrapperHandle)
	err = enc.Encode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return b, nil
}

func (c *uCodec) Decode(data []byte) (Event, error) {
	e := &uEvent{handle: c.bodyHandle}

	dec := ucodec.NewDecoderBytes(data, c.wrapperHandle)
	err := dec.Decode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return e, nil
}

func (e *uEvent) Headers() map[string]string {
	return e.HeadersMap
}

func (e *uEvent) Unmarshal(v interface{}) error {
	dec := ucodec.NewDecoderBytes(e.Body, e.handle)
	err := dec.Decode(v)
	if err != nil {
		return errors.WithStack(err)
	}

	return nil
}

func (c *rawCodec) Name() string {
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
