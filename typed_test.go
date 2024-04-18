package klevdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type strCodec struct{}

func (c strCodec) Encode(s string, has bool) ([]byte, error) {
	if has {
		return []byte(s), nil
	}
	return nil, nil
}

func (c strCodec) Decode(b []byte) (string, bool, error) {
	if b == nil {
		return "", false, nil
	}
	return string(b), true, nil
}

func TestKV(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenKV[string, string](dir, Options{}, strCodec{}, strCodec{})
	require.NoError(t, err)

	_, err = l.Publish([]KVMessage[string, string]{
		{
			Key:      "hello",
			HasValue: true,
			Value:    "world",
		},
		{
			Key: "hello again",
		},
	})
	require.NoError(t, err)

	_, msgs, err := l.Consume(OffsetOldest, 2)
	require.NoError(t, err)

	require.Equal(t, "hello", msgs[0].Key)
	require.True(t, msgs[0].HasValue)
	require.Equal(t, "world", msgs[0].Value)

	require.Equal(t, "hello again", msgs[1].Key)
	require.False(t, msgs[1].HasValue)
	require.Equal(t, "", msgs[1].Value)
}

func TestV(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenV[string](dir, Options{}, strCodec{})
	require.NoError(t, err)

	_, err = l.Publish([]VMessage[string]{
		{
			Value: "world",
		},
	})
	require.NoError(t, err)

	_, msgs, err := l.Consume(OffsetOldest, 1)
	require.NoError(t, err)

	require.Equal(t, "world", msgs[0].Value)
}
