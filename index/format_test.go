package index

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var indexSz = 10000

var iopts = NewParams(true, true)

func createIndex(dir string, itemCount int) (string, error) {
	var items = make([]Item, itemCount)
	for i := range items {
		items[i].Offset = int64(i)
		items[i].Position = int64(i)
		items[i].Timestamp = int64(i)
		items[i].KeyHash = uint64(i)
	}
	filename := filepath.Join(dir, "index")
	return filename, Write(filename, iopts, items)
}

func TestWriteRead(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz)
	require.NoError(t, err)

	items, err := Read(filename, iopts)
	require.NoError(t, err)
	require.Len(t, items, indexSz)

	for i, item := range items {
		require.Equal(t, int64(i), item.Offset)
		require.Equal(t, int64(i), item.Position)
		require.Equal(t, int64(i), item.Timestamp)
		require.Equal(t, uint64(i), item.KeyHash)
	}
}
