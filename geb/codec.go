package geb

import (
	"github.com/json-iterator/go"
	"github.com/pkg/errors"
)

// Codec is an interface for marshaling/unmarshaling event body and headers.
type Codec interface {
	// Name returns the name of the encoder, for debugging purposes.
	Name() string
	// NewEvent returns an empty event that can be marshalled into.
	NewEvent() codecEvent
	// Encode marshals the event into a byte stream.
	Encode(codecEvent) ([]byte, error)
	// Decode unmarhals a byte stream into a codecEvent.
	Decode(data []byte) (codecEvent, error)
}

type codecEvent interface {
	// Headers returns the event metadata.
	Headers() map[string]string
	// SetHeaders can be used to set metadata for the event.
	SetHeaders(map[string]string)
	// Unmarhal the event's body into the given argument v. V must be a non-nil pointer.
	Unmarshal(v interface{}) error
	// Marshal the given argument into the event's body.
	Marshal(v interface{}) error
}

type jsonCodec struct {
	wrapperJSON jsoniter.API
	bodyJSON    jsoniter.API
}

type jsonEvent struct {
	json       jsoniter.API
	Body       jsoniter.RawMessage `codec:"body"`
	HeadersMap map[string]string   `codec:"headers"`
}

type rawCodec struct{}

type rawEvent []byte

type CodecOption func(*codecSettings)

type codecSettings struct {
	tag string
}

// JSONCodec returns a codec that marshals both headers and body into a json object.
func JSONCodec(opts ...CodecOption) Codec {
	gs := &codecSettings{
		tag: "json",
	}
	for _, opt := range opts {
		opt(gs)
	}

	return &jsonCodec{
		wrapperJSON: jsoniter.Config{
			EscapeHTML:  true,
			SortMapKeys: true,
			TagKey:      "codec",
		}.Froze(),
		bodyJSON: jsoniter.Config{
			EscapeHTML:             true,
			SortMapKeys:            true,
			ValidateJsonRawMessage: true,
			OnlyTaggedField:        true,
			TagKey:                 gs.tag,
		}.Froze(),
	}
}

// RawCodec does not support headers, it simply sends the body as-is.
func RawCodec() Codec {
	return &rawCodec{}
}

// UseTag option can be used to change the struct tag used for marshaling/unmarshaling (default is json).
func UseTag(tag string) CodecOption {
	return func(gs *codecSettings) {
		gs.tag = tag
	}
}

func (c *jsonCodec) Name() string {
	return "json"
}

func (c *jsonCodec) NewEvent() codecEvent {
	return &jsonEvent{
		json:       c.bodyJSON,
		HeadersMap: make(map[string]string),
	}
}

func (c *jsonCodec) Encode(e codecEvent) ([]byte, error) {
	b, err := c.wrapperJSON.Marshal(e)
	if err != nil {
		return nil, errors.Wrapf(err, "geb.codec.Encode: %v", c.Name())
	}

	return b, nil
}

func (c *jsonCodec) Decode(data []byte) (codecEvent, error) {
	e := c.NewEvent()

	err := c.wrapperJSON.Unmarshal(data, e)
	if err != nil {
		return nil, errors.Wrapf(err, "geb.codec.Decode: %v", c.Name())
	}

	return e, nil
}

func (e *jsonEvent) SetHeaders(h map[string]string) {
	e.HeadersMap = h
}

func (e *jsonEvent) Headers() map[string]string {
	return e.HeadersMap
}

func (e *jsonEvent) Marshal(v interface{}) error {
	b, err := e.json.Marshal(v)
	if err != nil {
		return errors.Wrapf(err, "geb.codec.Marshal: json")
	}
	e.Body = b

	return nil
}

func (e *jsonEvent) Unmarshal(v interface{}) error {
	if e.Body == nil {
		return errors.Errorf("geb.codec.Unmarshal: json: tried to unmarshal nil body")
	}

	err := e.json.Unmarshal(e.Body, v)
	if err != nil {
		return errors.Wrapf(err, "geb.codec.Unmarshal: json")
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
