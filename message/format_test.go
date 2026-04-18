package message

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLogWriteRead(t *testing.T) {
	msgs := Gen(2)
	for i := range msgs {
		msgs[i].Offset = int64(i + 5)
	}

	path := filepath.Join(t.TempDir(), "test.log")
	w, err := OpenWriter(path, FormatLog)
	require.NoError(t, err)

	pos, err := w.Write(msgs[0])
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	pos, err = w.Write(msgs[1])
	require.NoError(t, err)
	require.Equal(t, Size(msgs[0], FormatLog), pos)

	err = w.SyncAndClose()
	require.NoError(t, err)

	t.Run("Direct", func(t *testing.T) {
		r, err := OpenReader(path, FormatLog)
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
		r, err := OpenReaderMem(path, FormatLog)
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

func TestSegmentWriteRead(t *testing.T) {
	msgs := Gen(2)
	for i := range msgs {
		msgs[i].Offset = int64(i + 5)
	}

	path := filepath.Join(t.TempDir(), "test.segment")
	w, err := OpenWriter(path, FormatSegment)
	require.NoError(t, err)

	pos, err := w.Write(msgs[0])
	require.NoError(t, err)
	require.Equal(t, HeaderSize, pos)

	pos, err = w.Write(msgs[1])
	require.NoError(t, err)
	require.Equal(t, HeaderSize+Size(msgs[0], FormatSegment), pos)

	err = w.SyncAndClose()
	require.NoError(t, err)

	t.Run("Direct", func(t *testing.T) {
		r, err := OpenReader(path, FormatSegment)
		require.NoError(t, err)
		msg, err := r.Get(int64(HeaderSize))
		require.NoError(t, err)
		require.Equal(t, msgs[0], msg)
		require.NoError(t, r.Close())
	})

	t.Run("Mem", func(t *testing.T) {
		r, err := OpenReaderMem(path, FormatSegment)
		require.NoError(t, err)
		msg, err := r.Get(int64(HeaderSize))
		require.NoError(t, err)
		require.Equal(t, msgs[0], msg)
		require.NoError(t, r.Close())
	})
}

func TestSegmentInvalidHeader(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.segment")
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte("badmagic"))
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Run("Direct", func(t *testing.T) {
		_, err := OpenReader(path, FormatSegment)
		require.ErrorIs(t, err, ErrCorrupted)
	})

	t.Run("Mem", func(t *testing.T) {
		_, err := OpenReaderMem(path, FormatSegment)
		require.ErrorIs(t, err, ErrCorrupted)
	})
}

func TestPartialMessageHeader(t *testing.T) {
	// A 1-byte file simulates a crash mid-header-write; OpenReader must return ErrCorrupted.
	path := filepath.Join(t.TempDir(), "test.segment")
	f, err := os.Create(path)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF}) // just one byte of the 8-byte magic
	require.NoError(t, err)
	require.NoError(t, f.Close())

	t.Run("Direct", func(t *testing.T) {
		_, err := OpenReader(path, FormatSegment)
		require.ErrorIs(t, err, ErrCorrupted)
	})

	t.Run("Mem", func(t *testing.T) {
		_, err := OpenReaderMem(path, FormatSegment)
		require.ErrorIs(t, err, ErrCorrupted)
	})
}

func TestSegmentReadv1LargeTotalLength(t *testing.T) {
	// A corrupt totalLength = 0x7FFFFFFF passes the negative guard but must be
	// rejected before attempting a ~2 GB allocation.
	path := filepath.Join(t.TempDir(), "test.segment")
	f, err := os.Create(path)
	require.NoError(t, err)

	_, err = f.Write(headerNew(v1))
	require.NoError(t, err)

	// Crafted message header: totalLength=0x7FFFFFFF, valueSize=0 — CRC left as
	// zeros (the size guard fires before the CRC check).
	var hdr [msgHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[4:], 0x7FFFFFFF) // totalLength = max int32
	binary.BigEndian.PutUint32(hdr[24:], 0)          // valueSize = 0
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := OpenReader(path, FormatSegment)
	require.NoError(t, err)
	defer r.Close()

	_, _, err = r.Read(r.InitialPosition())
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestSegmentReadv1NegativeValueSize(t *testing.T) {
	// Regression test: a negative valueSize (high bit set in the uint32 field) must
	// be rejected as ErrCorrupted, not cause a slice-bounds panic.
	// With totalLength=28 and valueSize=int32(-9), keySize derives as 9 but dataSize
	// is only 8 — without the valueSize<0 guard, msg.Key = dataBytes[:9] panics.
	path := filepath.Join(t.TempDir(), "test.segment")
	f, err := os.Create(path)
	require.NoError(t, err)

	// Valid file header
	_, err = f.Write(headerNew(v1))
	require.NoError(t, err)

	// Crafted v1 message header: totalLength=28, offset=0, time=0, valueSize=0xFFFFFFF7
	// CRC left as zeros — the guard fires before the CRC check.
	var hdr [msgHeaderSize]byte
	binary.BigEndian.PutUint32(hdr[4:], uint32(totalLengthFixed)) // totalLength = 28
	binary.BigEndian.PutUint32(hdr[24:], 0xFFFFFFF7)              // valueSize = int32(-9)
	_, err = f.Write(hdr[:])
	require.NoError(t, err)
	require.NoError(t, f.Close())

	r, err := OpenReader(path, FormatSegment)
	require.NoError(t, err)
	defer r.Close()

	_, _, err = r.Read(r.InitialPosition())
	require.ErrorIs(t, err, ErrCorrupted)
}

func TestLogSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.log")
	w, err := OpenWriter(path, FormatLog)
	require.NoError(t, err)

	msg := Message{
		Key:   []byte("abc"),
		Value: []byte("abcde"),
	}
	pos, err := w.Write(msg)
	require.NoError(t, err)
	require.Equal(t, int64(0), pos)

	require.Equal(t, w.pos, Size(msg, FormatLog))
}

func TestSegmentSize(t *testing.T) {
	path := filepath.Join(t.TempDir(), "test.segment")
	w, err := OpenWriter(path, FormatSegment)
	require.NoError(t, err)

	msg := Message{
		Key:   []byte("abc"),
		Value: []byte("abcde"),
	}
	pos, err := w.Write(msg)
	require.NoError(t, err)
	require.Equal(t, HeaderSize, pos)

	require.Equal(t, w.pos, HeaderSize+Size(msg, FormatSegment))
}
