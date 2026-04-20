package index

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var indexSz = 10000

var iopts = Params{true, true}

func createIndex(dir string, itemCount int, v Version) (string, error) {
	var items = make([]Item, itemCount)
	for i := range items {
		items[i].Offset = int64(i)
		items[i].Position = int64(i)
		items[i].Timestamp = int64(i)
		items[i].KeyHash = uint64(i)
	}
	filename := filepath.Join(dir, "index")
	return filename, Write(filename, 0, v, iopts, items)
}

func TestWriteReadV1(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz, V1)
	require.NoError(t, err)

	items, err := Read(filename, 0, iopts)
	require.NoError(t, err)
	require.Len(t, items, indexSz)

	for i, item := range items {
		require.Equal(t, int64(i), item.Offset)
		require.Equal(t, int64(i), item.Position)
		require.Equal(t, int64(i), item.Timestamp)
		require.Equal(t, uint64(i), item.KeyHash)
	}
}
func TestWriteReadV2(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz, V2)
	require.NoError(t, err)

	got, err := Read(filename, 0, iopts)
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

	_, err = Read(path, 0, iopts)
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

	_, err = Read(path, 0, iopts)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestStat(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "index")
		f, err := os.Create(path)
		require.NoError(t, err)
		require.NoError(t, f.Close())

		size, count, err := Stat(path, 0, iopts)
		require.NoError(t, err)
		require.Equal(t, int64(0), size)
		require.Equal(t, 0, count)
	})

	t.Run("V1", func(t *testing.T) {
		dir := t.TempDir()
		filename, err := createIndex(dir, indexSz, V1)
		require.NoError(t, err)

		size, count, err := Stat(filename, 0, iopts)
		require.NoError(t, err)
		require.Equal(t, int64(indexSz)*iopts.Size(), size)
		require.Equal(t, indexSz, count)
	})

	t.Run("V2", func(t *testing.T) {
		dir := t.TempDir()
		filename, err := createIndex(dir, indexSz, V2)
		require.NoError(t, err)

		size, count, err := Stat(filename, 0, iopts)
		require.NoError(t, err)
		require.Equal(t, HeaderSize+int64(indexSz)*iopts.Size(), size)
		require.Equal(t, indexSz, count)
	})
}
