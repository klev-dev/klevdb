package message

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

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
