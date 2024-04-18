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
