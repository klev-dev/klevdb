package klevdb

import (
	"testing"

	"github.com/stretchr/testify/require"
)

type tobj struct {
	V string `json:"v"`
}

func TestKV(t *testing.T) {
	dir := t.TempDir()
	l, err := OpenT[tobj, tobj](dir, Options{}, JsonCodec[tobj]{}, JsonCodec[tobj]{})
	require.NoError(t, err)

	_, err = l.Publish([]TMessage[tobj, tobj]{
		{
			Key:   tobj{"hello"},
			Value: tobj{"world"},
		},
		{
			Key:      tobj{"hello"},
			KeyEmpty: true,
			Value:    tobj{"world"},
		},
		{
			Key:        tobj{"hello"},
			Value:      tobj{"world"},
			ValueEmpty: true,
		},
		{
			Key:        tobj{"hello"},
			KeyEmpty:   true,
			Value:      tobj{"world"},
			ValueEmpty: true,
		},
	})
	require.NoError(t, err)

	_, msgs, err := l.Consume(OffsetOldest, 4)
	require.NoError(t, err)

	require.Equal(t, tobj{"hello"}, msgs[0].Key)
	require.False(t, msgs[0].KeyEmpty)
	require.Equal(t, tobj{"world"}, msgs[0].Value)
	require.False(t, msgs[0].ValueEmpty)

	require.Equal(t, tobj{""}, msgs[1].Key)
	require.True(t, msgs[1].KeyEmpty)
	require.Equal(t, tobj{"world"}, msgs[1].Value)
	require.False(t, msgs[1].ValueEmpty)

	require.Equal(t, tobj{"hello"}, msgs[2].Key)
	require.False(t, msgs[2].KeyEmpty)
	require.Equal(t, tobj{""}, msgs[2].Value)
	require.True(t, msgs[2].ValueEmpty)

	require.Equal(t, tobj{""}, msgs[3].Key)
	require.True(t, msgs[3].KeyEmpty)
	require.Equal(t, tobj{""}, msgs[3].Value)
	require.True(t, msgs[3].ValueEmpty)
}
