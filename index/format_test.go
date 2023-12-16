package index

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

var indexSz = 10000

func createIndex(dir string, itemCount int) (string, error) {
	var items = make([]TimeKeyItem, itemCount)
	for i := range items {
		items[i].offset = int64(i)
		items[i].position = int64(i)
		items[i].timestamp = int64(i)
		items[i].keyHash = uint64(i)
	}
	filename := filepath.Join(dir, "index")
	return filename, Write(filename, TimeKeyIndex{}, items)
}

func TestWriteRead(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz)
	require.NoError(t, err)

	items, err := Read(filename, TimeKeyIndex{})
	require.NoError(t, err)
	require.Len(t, items, indexSz)

	for i, item := range items {
		require.Equal(t, int64(i), item.offset)
		require.Equal(t, int64(i), item.position)
		require.Equal(t, int64(i), item.timestamp)
		require.Equal(t, uint64(i), item.keyHash)
	}
}
