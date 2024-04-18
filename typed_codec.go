package klevdb

import "encoding/json"

type Codec[T any] interface {
	Encode(T) ([]byte, error)
	Decode([]byte) (T, error)
}

type OptCodec[T any] interface {
	EncodeOpt(T, bool) ([]byte, error)
	DecodeOpt([]byte) (T, bool, error)
}

type JsonCodec[T any] struct{}

func (c JsonCodec[T]) Encode(t T) ([]byte, error) {
	return json.Marshal(t)
}

func (c JsonCodec[T]) Decode(b []byte) (T, error) {
	var t T
	err := json.Unmarshal(b, &t)
	return t, err
}

func (c JsonCodec[T]) EncodeOpt(t T, has bool) ([]byte, error) {
	if has {
		return json.Marshal(t)
	}
	return nil, nil
}

func (c JsonCodec[T]) DecodeOpt(b []byte) (T, bool, error) {
	var t T
	if b == nil {
		return t, false, nil
	}
	err := json.Unmarshal(b, &t)
	return t, true, err
}

type stringCodec struct{}

func (c stringCodec) Encode(t string) ([]byte, error) {
	return []byte(t), nil
}

func (c stringCodec) Decode(b []byte) (string, error) {
	return string(b), nil
}

func (c stringCodec) EncodeOpt(t string, has bool) ([]byte, error) {
	if has {
		return json.Marshal(t)
	}
	return nil, nil
}

func (c stringCodec) DecodeOpt(b []byte) (string, bool, error) {
	if b == nil {
		return "", false, nil
	}
	var s string
	err := json.Unmarshal(b, &s)
	return s, true, err
}

var StringCodec = stringCodec{}

type stringBareCodec struct{}

func (c stringBareCodec) EncodeOpt(t string, has bool) ([]byte, error) {
	return []byte(t), nil
}

func (c stringBareCodec) DecodeOpt(b []byte) (string, bool, error) {
	return string(b), true, nil
}

var StringBareCodec = stringBareCodec{}
