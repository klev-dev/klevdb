package klevdb

import (
	"encoding/binary"
	"encoding/json"

	"github.com/klev-dev/kleverr"
)

// Codec is interface satisfied by all codecs
type Codec[T any] interface {
	Encode(t T, empty bool) (b []byte, err error)
	Decode(b []byte) (t T, empty bool, err error)
}

// JsonCodec supports coding values as a JSON
type JsonCodec[T any] struct{}

func (c JsonCodec[T]) Encode(t T, empty bool) ([]byte, error) {
	if empty {
		return nil, nil
	}
	return json.Marshal(t)
}

func (c JsonCodec[T]) Decode(b []byte) (T, bool, error) {
	var t T
	if b == nil {
		return t, true, nil
	}
	err := json.Unmarshal(b, &t)
	return t, false, err
}

type stringOptCodec struct{}

func (c stringOptCodec) Encode(t string, empty bool) ([]byte, error) {
	if empty {
		return nil, nil
	}
	return json.Marshal(t)
}

func (c stringOptCodec) Decode(b []byte) (string, bool, error) {
	if b == nil {
		return "", true, nil
	}
	var s string
	err := json.Unmarshal(b, &s)
	return s, false, err
}

// StringOptCodec supports coding an optional string, e.g. differantiates between "" and nil strings
var StringOptCodec = stringOptCodec{}

type stringCodec struct{}

func (c stringCodec) Encode(t string, empty bool) ([]byte, error) {
	return []byte(t), nil
}

func (c stringCodec) Decode(b []byte) (string, bool, error) {
	return string(b), false, nil
}

// StringCodec supports coding a string
var StringCodec = stringCodec{}

type varintCodec struct{}

func (c varintCodec) Encode(t int64, empty bool) ([]byte, error) {
	if empty {
		return nil, nil
	}
	return binary.AppendVarint(nil, t), nil
}

func (c varintCodec) Decode(b []byte) (int64, bool, error) {
	if b == nil {
		return 0, true, nil
	}
	t, n := binary.Varint(b)
	if n <= 0 {
		return 0, true, kleverr.Newf("invalid varint: %d", n)
	}
	return t, false, nil
}

// VarintCodec supports coding integers as varint
var VarintCodec = varintCodec{}
