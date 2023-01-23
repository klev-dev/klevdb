package message

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteRead(t *testing.T) {
	msgs := Gen(2)
	for i := range msgs {
		msgs[i].Offset = int64(i + 5)
	}

	path := filepath.Join(t.TempDir(), "test.log")
	w, err := OpenWriter(path)
	require.NoError(t, err)

	pos, err := w.Write(msgs[0])
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	pos, err = w.Write(msgs[1])
	require.NoError(t, err)
	require.Equal(t, Size(msgs[0]), pos)

	err = w.SyncAndClose()
	require.NoError(t, err)

	t.Run("Direct", func(t *testing.T) {
		r, err := OpenReader(path)
		require.NoError(t, err)

		msg, err := r.Get(0)
		require.NoError(t, err)
		require.Equal(t, msgs[0], msg)

		msg, err = r.Get(pos)
		require.NoError(t, err)
		require.Equal(t, msgs[1], msg)

		err = r.Close()
		require.NoError(t, err)
	})

	t.Run("Mem", func(t *testing.T) {
		r, err := OpenReaderMem(path)
		require.NoError(t, err)

		msg, err := r.Get(0)
		require.NoError(t, err)
		require.Equal(t, msgs[0], msg)

		msg, err = r.Get(pos)
		require.NoError(t, err)
		require.Equal(t, msgs[1], msg)

		err = r.Close()
		require.NoError(t, err)
	})
}

func TestLogSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	w, err := OpenWriter(path)
	require.NoError(t, err)

	msg := Message{
		Key:   []byte("abc"),
		Value: []byte("abcde"),
	}
	pos, err := w.Write(msg)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	require.Equal(t, w.pos, Size(msg))
}
