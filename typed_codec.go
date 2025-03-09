package klevdb

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
)

type Codec[T any] interface {
	Encode(t T, empty bool) (b []byte, err error)
	Decode(b []byte) (t T, empty bool, err error)
}

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

var StringOptCodec = stringOptCodec{}

type stringCodec struct{}

func (c stringCodec) Encode(t string, empty bool) ([]byte, error) {
	return []byte(t), nil
}

func (c stringCodec) Decode(b []byte) (string, bool, error) {
	return string(b), false, nil
}

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
		return 0, true, fmt.Errorf("[klevdb.VarintCodec.Decode] invalid varint %d", n)
	}
	return t, false, nil
}

var VarintCodec = varintCodec{}
