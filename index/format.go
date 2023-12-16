package index

import (
	"errors"
	"io"
	"os"

	"github.com/klev-dev/kleverr"
)

var ErrCorrupted = errors.New("index corrupted")

type Writer struct {
	opts Index[Item]
	f    *os.File
	pos  int64
	buff []byte
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
		opts: opts,
		f:    f,
		pos:  pos,
	}, nil
}

func (w *Writer) Write(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, w.opts.Size())
	}

	if err := w.opts.Write(it, w.buff); err != nil {
		return err
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

func Read(path string, opts Index[Item]) ([]Item, error) {
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

	var items = make([]Item, dataSize/int64(itemSize))
	for i := range items {
		pos := i * int(itemSize)

		if item, err := opts.Read(data[pos:]); err != nil {
			return nil, kleverr.Newf("could not read index: %w", err)
		} else {
			items[i] = item
		}
	}
	return items, nil
}
