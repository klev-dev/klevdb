package klevdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestKV(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenKV[string, string](dir, Options{}, StringCodec, StringCodec)
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
		{
			Key:      "hello more",
			HasValue: true,
		},
	})
	require.NoError(t, err)

	_, msgs, err := l.Consume(OffsetOldest, 3)
	require.NoError(t, err)

	require.Equal(t, "hello", msgs[0].Key)
	require.True(t, msgs[0].HasValue)
	require.Equal(t, "world", msgs[0].Value)

	require.Equal(t, "hello again", msgs[1].Key)
	require.False(t, msgs[1].HasValue)
	require.Equal(t, "", msgs[1].Value)

	require.Equal(t, "hello more", msgs[2].Key)
	require.True(t, msgs[2].HasValue)
	require.Equal(t, "", msgs[2].Value)
}

func TestV(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenV[string](dir, Options{}, StringCodec)
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
