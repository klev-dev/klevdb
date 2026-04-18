package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var indexSz = 10000

var iopts = Params{true, true}

func createIndex(dir string, itemCount int) (string, error) {
	var items = make([]Item, itemCount)
	for i := range items {
		items[i].Offset = int64(i)
		items[i].Position = int64(i)
		items[i].Timestamp = int64(i)
		items[i].KeyHash = uint64(i)
	}
	filename := filepath.Join(dir, "index")
	return filename, Write(filename, iopts, items, FormatLog)
}

func TestWriteReadWithHeader(t *testing.T) {
	dir := t.TempDir()

	var items = make([]Item, indexSz)
	for i := range items {
		items[i].Offset = int64(i)
		items[i].Position = int64(i)
		items[i].Timestamp = int64(i)
		items[i].KeyHash = uint64(i)
	}
	filename := filepath.Join(dir, "index.segment")
	require.NoError(t, Write(filename, iopts, items, FormatSegment))

	got, err := Read(filename, iopts, FormatSegment)
	require.NoError(t, err)
	require.Len(t, got, indexSz)

	for i, item := range got {
		require.Equal(t, int64(i), item.Offset)
		require.Equal(t, int64(i), item.Position)
		require.Equal(t, int64(i), item.Timestamp)
		require.Equal(t, uint64(i), item.KeyHash)
	}
}

func TestInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "index.segment")
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte("badmagic"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Read(path, iopts, FormatSegment)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestPartialIndexHeader(t *testing.T) {
	// A 1-byte file simulates a crash mid-header-write; Read must return ErrCorrupted.
	path := filepath.Join(t.TempDir(), "index.segment")
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF}) // just one byte of the 8-byte magic
	require.NoError(t, err)
	require.NoError(t, f.Close())

	_, err = Read(path, iopts, FormatSegment)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestWriteRead(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz)
	require.NoError(t, err)

	items, err := Read(filename, iopts, FormatLog)
	require.NoError(t, err)
	require.Len(t, items, indexSz)

	for i, item := range items {
		require.Equal(t, int64(i), item.Offset)
		require.Equal(t, int64(i), item.Position)
		require.Equal(t, int64(i), item.Timestamp)
		require.Equal(t, uint64(i), item.KeyHash)
	}
}
