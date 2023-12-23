package index

import (
	"encoding/binary"
	"errors"
	"io"
	"os"

	"github.com/klev-dev/kleverr"
)

var ErrCorrupted = errors.New("index corrupted")

type Writer struct {
	opts      Params
	f         *os.File
	pos       int64
	buff      []byte
	keyOffset int
}

func OpenWriter(path string, opts Params) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, kleverr.Newf("could not open index: %w", err)
	}

	pos := int64(0)
	if stat, err := f.Stat(); err != nil {
		return nil, kleverr.Newf("could not stat index: %w", err)
	} else {
		pos = stat.Size()
	}

	return &Writer{
		opts:      opts,
		f:         f,
		pos:       pos,
		keyOffset: opts.keyOffset(),
	}, nil
}

func (w *Writer) Write(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, w.opts.Size())
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))

	if w.opts.Times {
		binary.BigEndian.PutUint64(w.buff[16:], uint64(it.Timestamp))
	}

	if w.opts.Keys {
		copy(w.buff[w.keyOffset:], it.KeyHash[:])
	}

	if n, err := w.f.Write(w.buff); err != nil {
		return kleverr.Newf("failed to write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer) Size() int64 {
	return w.pos
}

func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return kleverr.Newf("could not sync index: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close index: %w", err)
	}
	return nil
}

func (w *Writer) SyncAndClose() error {
	if err := w.f.Sync(); err != nil {
		return kleverr.Newf("could not sync index: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close index: %w", err)
	}
	return nil
}

func Write(path string, opts Params, index []Item) error {
	w, err := OpenWriter(path, opts)
	if err != nil {
		return err
	}
	defer w.Close()

	for _, item := range index {
		if err := w.Write(item); err != nil {
			return err
		}
	}

	return w.SyncAndClose()
}

func Read(path string, opts Params) ([]Item, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, kleverr.Newf("could not open index: %w", err)
	}
	defer f.Close()

	stat, err := os.Stat(path)
	if err != nil {
		return nil, kleverr.Newf("could not stat index: %w", err)
	}
	dataSize := stat.Size()

	itemSize := opts.Size()
	if dataSize%itemSize > 0 {
		return nil, kleverr.Newf("%w: unexpected data len: %d", ErrCorrupted, dataSize)
	}

	data := make([]byte, dataSize)
	if _, err = io.ReadFull(f, data); err != nil {
		return nil, kleverr.Newf("could not read index: %w", err)
	}

	var keyOffset = opts.keyOffset()

	var items = make([]Item, dataSize/int64(itemSize))
	for i := range items {
		pos := i * int(itemSize)

		items[i].Offset = int64(binary.BigEndian.Uint64(data[pos:]))
		items[i].Position = int64(binary.BigEndian.Uint64(data[pos+8:]))

		if opts.Times {
			items[i].Timestamp = int64(binary.BigEndian.Uint64(data[pos+16:]))
		}

		if opts.Keys {
			copy(items[i].KeyHash[:], data[pos+keyOffset:])
		}
	}
	return items, nil
}
