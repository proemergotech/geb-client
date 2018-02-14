package geb

import (
	"github.com/pkg/errors"
	ucodec "github.com/ugorji/go/codec"
)

type Codec interface {
	Name() string
	NewEvent() codecEvent
	Encode(codecEvent) ([]byte, error)
	Decode(data []byte) (codecEvent, error)
}

type codecEvent interface {
	Headers() map[string]string
	SetHeaders(map[string]string)
	Unmarshal(v interface{}) error
	Marshal(v interface{}) error
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

func (c *uCodec) NewEvent() codecEvent {
	return &uEvent{handle: c.bodyHandle}
}

func (c *uCodec) Encode(e codecEvent) ([]byte, error) {
	b := []byte(nil)
	enc := ucodec.NewEncoderBytes(&b, c.wrapperHandle)
	err := enc.Encode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return b, nil
}

func (c *uCodec) Decode(data []byte) (codecEvent, error) {
	e := c.NewEvent()

	dec := ucodec.NewDecoderBytes(data, c.wrapperHandle)
	err := dec.Decode(e)
	if err != nil {
		return nil, errors.WithStack(err)
	}

	return e, nil
}

func (e *uEvent) SetHeaders(h map[string]string) {
	e.HeadersMap = h
	return
}

func (e *uEvent) Headers() map[string]string {
	return e.HeadersMap
}

func (e *uEvent) Marshal(v interface{}) error {
	b := []byte(nil)
	enc := ucodec.NewEncoderBytes(&b, e.handle)
	err := enc.Encode(v)
	if err != nil {
		return errors.WithStack(err)
	}
	e.Body = b

	return nil
}

func (e *uEvent) Unmarshal(v interface{}) error {
	if e.Body == nil {
		return errors.New("geb.codec.Unmarshal: tried to unmarshal nil body")
	}

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

func (c *rawCodec) NewEvent() codecEvent {
	return new(rawEvent)
}

func (*rawCodec) Encode(e codecEvent) ([]byte, error) {
	var b []byte
	err := e.Unmarshal(&b)
	if err != nil {
		return nil, err
	}

	return b, nil
}

func (*rawCodec) Decode(data []byte) (codecEvent, error) {
	r := new(rawEvent)
	*r = data
	return r, nil
}

func (*rawEvent) SetHeaders(map[string]string) {
	return
}

func (*rawEvent) Headers() map[string]string {
	return nil
}

func (r *rawEvent) Marshal(v interface{}) error {
	vB, ok := v.([]byte)
	if !ok {
		return errors.New("geb.rawEvent.Marshal: v must be []byte in raw mode")
	}

	*r = vB

	return nil
}

func (r *rawEvent) Unmarshal(v interface{}) error {
	vB, ok := v.(*[]byte)
	if !ok || vB == nil {
		return errors.New("geb.rawEvent.Unmarshal: v must be a non-nil *[]byte in raw mode")
	}

	*vB = *r

	return nil
}
