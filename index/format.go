package index

import (
	"errors"
	"io"
	"os"

	"github.com/klev-dev/kleverr"
)

var ErrCorrupted = errors.New("index corrupted")

type Writer[IX Index[IT, IC], IT IndexItem, IC IndexContext] struct {
	ix   IX
	f    *os.File
	pos  int64
	buff []byte
}

func OpenWriter[IX Index[IT, IC], IT IndexItem, IC IndexContext](path string, ix IX) (*Writer[IX, IT, IC], error) {
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

	return &Writer[IX, IT, IC]{
		ix:  ix,
		f:   f,
		pos: pos,
	}, nil
}

func (w *Writer[IX, IT, IC]) Write(it IT) error {
	if w.buff == nil {
		w.buff = make([]byte, w.ix.Size())
	}

	if err := w.ix.Write(it, w.buff); err != nil {
		return err
	}

	if n, err := w.f.Write(w.buff); err != nil {
		return kleverr.Newf("failed to write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer[IX, IT, IC]) Size() int64 {
	return w.pos
}

func (w *Writer[IX, IT, IC]) Sync() error {
	if err := w.f.Sync(); err != nil {
		return kleverr.Newf("could not sync index: %w", err)
	}
	return nil
}

func (w *Writer[IX, IT, IC]) Close() error {
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close index: %w", err)
	}
	return nil
}

func (w *Writer[IX, IT, IC]) SyncAndClose() error {
	if err := w.f.Sync(); err != nil {
		return kleverr.Newf("could not sync index: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close index: %w", err)
	}
	return nil
}

func Write[IX Index[IT, IC], IT IndexItem, IC IndexContext](path string, ix IX, items []IT) error {
	w, err := OpenWriter(path, ix)
	if err != nil {
		return err
	}
	defer w.Close()

	for _, item := range items {
		if err := w.Write(item); err != nil {
			return err
		}
	}

	return w.SyncAndClose()
}

func Read[IX Index[IT, IC], IT IndexItem, IC IndexContext](path string, ix IX) ([]IT, error) {
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

	itemSize := ix.Size()
	if dataSize%itemSize > 0 {
		return nil, kleverr.Newf("%w: unexpected data len: %d", ErrCorrupted, dataSize)
	}

	data := make([]byte, dataSize)
	if _, err = io.ReadFull(f, data); err != nil {
		return nil, kleverr.Newf("could not read index: %w", err)
	}

	var items = make([]IT, dataSize/int64(itemSize))
	for i := range items {
		pos := i * int(itemSize)

		if item, err := ix.Read(data[pos:]); err != nil {
			return nil, kleverr.Newf("could not read index: %w", err)
		} else {
			items[i] = item
		}
	}
	return items, nil
}
