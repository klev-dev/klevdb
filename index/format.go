package index

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var ErrCorrupted = errors.New("index corrupted")
var errIndexSize = fmt.Errorf("%w: unaligned index size", ErrCorrupted)

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
		return nil, fmt.Errorf("write index open: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("write index stat: %w", err)
	}

	return &Writer{
		opts:      opts,
		f:         f,
		pos:       stat.Size(),
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
		binary.BigEndian.PutUint64(w.buff[w.keyOffset:], it.KeyHash)
	}

	if n, err := w.f.Write(w.buff); err != nil {
		return fmt.Errorf("write index: %w", err)
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
		return fmt.Errorf("write index sync: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("write index close: %w", err)
	}
	return nil
}

func (w *Writer) SyncAndClose() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.Close()
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
		return nil, fmt.Errorf("read index open: %w", err)
	}
	defer f.Close()

	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read index stat: %w", err)
	}
	dataSize := stat.Size()

	itemSize := opts.Size()
	if dataSize%itemSize > 0 {
		return nil, errIndexSize
	}

	data := make([]byte, dataSize)
	if _, err = io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
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
			items[i].KeyHash = binary.BigEndian.Uint64(data[pos+keyOffset:])
		}
	}
	return items, nil
}
