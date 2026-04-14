package index

import (
	"encoding/binary"
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
	return filename, Write(filename, iopts, items)
}

func TestWriteRead(t *testing.T) {
	dir := t.TempDir()

	filename, err := createIndex(dir, indexSz)
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

func TestRead_HeaderOnly(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, Write(path, iopts, nil))

	items, err := Read(path, 0, iopts)
	require.NoError(t, err)
	require.Empty(t, items)
}

func TestRead_ParamsMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, Write(path, Params{Times: true, Keys: false}, nil))

	_, err := Read(path, 0, Params{Times: true, Keys: true})
	require.ErrorIs(t, err, ErrParamsMismatch)
}

func TestRead_Corrupted(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, []byte("notaheader1234567890"), 0600))

	_, err := Read(path, 0, iopts)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestRead_PartialHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, magic[:4], 0600))

	_, err := Read(path, 0, iopts)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestWrite_OverwritesExistingFile(t *testing.T) {
	dir := t.TempDir()

	// Write with mismatched params (old format simulation)
	path := filepath.Join(dir, "index")
	require.NoError(t, Write(path, Params{Times: false, Keys: false}, []Item{{Offset: 99}}))

	// Overwrite with new params — should succeed (O_TRUNC, not O_APPEND)
	require.NoError(t, Write(path, iopts, []Item{{Offset: 1, Position: 2, Timestamp: 3, KeyHash: 4}}))

	items, err := Read(path, 0, iopts)
	require.NoError(t, err)
	require.Len(t, items, 1)
	require.Equal(t, int64(1), items[0].Offset)
}

func TestOpenWriter_ExistingValid(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	// Create index with one item
	require.NoError(t, Write(path, iopts, []Item{{Offset: 0}}))

	// Reopen and append another item
	w, err := OpenWriter(path, 0, iopts)
	require.NoError(t, err)
	require.NoError(t, w.Write(Item{Offset: 1, Position: 1, Timestamp: 1, KeyHash: 1}))
	require.NoError(t, w.SyncAndClose())

	items, err := Read(path, 0, iopts)
	require.NoError(t, err)
	require.Len(t, items, 2)
}

func TestOpenWriter_BadHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, make([]byte, HeaderSize), 0600))

	_, err := OpenWriter(path, 0, iopts)
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestOpenWriter_ParamsMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, Write(path, Params{Times: false, Keys: false}, nil))

	_, err := OpenWriter(path, 0, iopts)
	require.ErrorIs(t, err, ErrParamsMismatch)
}

func TestNeedsReindex_Missing(t *testing.T) {
	dir := t.TempDir()
	needs, err := NeedsReindex(filepath.Join(dir, "index"), iopts)
	require.NoError(t, err)
	require.True(t, needs)
}

func TestNeedsReindex_Empty(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, nil, 0600))

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.True(t, needs)
}

func TestNeedsReindex_ValidHeader(t *testing.T) {
	dir := t.TempDir()
	path, err := createIndex(dir, 10)
	require.NoError(t, err)

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.False(t, needs)
}

func TestNeedsReindex_BadPreamble(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, make([]byte, HeaderSize), 0600))

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.True(t, needs)
}

func TestNeedsReindex_ParamsMismatch(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, Write(path, Params{Times: false, Keys: false}, nil))

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.True(t, needs)
}

func TestNeedsReindex_PartialHeader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "index")
	require.NoError(t, os.WriteFile(path, magic[:4], 0600)) // 4 bytes, less than headerSize

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.True(t, needs)
}

// writeV0Index writes a header-less (pre-v1) index file directly using raw binary encoding.
func writeV0Index(path string, opts Params, items []Item) error {
	itemSize := int(opts.Size())
	keyOffset := opts.keyOffset()
	data := make([]byte, len(items)*itemSize)
	for i, it := range items {
		pos := i * itemSize
		binary.BigEndian.PutUint64(data[pos:], uint64(it.Offset))
		binary.BigEndian.PutUint64(data[pos+8:], uint64(it.Position))
		if opts.Times {
			binary.BigEndian.PutUint64(data[pos+16:], uint64(it.Timestamp))
		}
		if opts.Keys {
			binary.BigEndian.PutUint64(data[pos+keyOffset:], it.KeyHash)
		}
	}
	return os.WriteFile(path, data, 0600)
}

func TestRead_V0Compat(t *testing.T) {
	const segOffset = int64(100)
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	want := []Item{
		{Offset: segOffset, Position: 0, Timestamp: 1, KeyHash: 11},
		{Offset: segOffset + 1, Position: 100, Timestamp: 2, KeyHash: 22},
	}
	require.NoError(t, writeV0Index(path, iopts, want))

	items, err := Read(path, segOffset, iopts)
	require.NoError(t, err)
	require.Equal(t, want, items)
}

func TestNeedsReindex_V0(t *testing.T) {
	const segOffset = int64(100)
	dir := t.TempDir()
	path := filepath.Join(dir, "index")

	require.NoError(t, writeV0Index(path, iopts, []Item{
		{Offset: segOffset, Position: 0, Timestamp: 1, KeyHash: 11},
	}))

	needs, err := NeedsReindex(path, iopts)
	require.NoError(t, err)
	require.True(t, needs)
}
