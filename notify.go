package klevdb

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
)

var ErrOffsetNotifyClosed = errors.New("offset notify already closed")

type OffsetNotify struct {
	nextOffset atomic.Int64
	barrier    chan chan struct{}
}

func NewOffsetNotify(nextOffset int64) *OffsetNotify {
	w := &OffsetNotify{
		barrier: make(chan chan struct{}, 1),
	}

	w.nextOffset.Store(nextOffset)
	w.barrier <- make(chan struct{})

	return w
}

func (w *OffsetNotify) Wait(ctx context.Context, offset int64) error {
	// quick path, just load and check
	if w.nextOffset.Load() > offset {
		return nil
	}

	// acquire current barrier
	b, ok := <-w.barrier
	if !ok {
		// already closed, return error
		return fmt.Errorf("[klevdb.OffsetNotify.Read] aquire barrier: %w", ErrOffsetNotifyClosed)
	}

	// probe the current offset
	updated := w.nextOffset.Load() > offset

	// release current barrier
	w.barrier <- b

	// already has a new value, return
	if updated {
		return nil
	}

	// now wait for something to happen
	select {
	case <-b:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("[klevdb.OffsetNotify.Read] wait canceled: %w", ctx.Err())
	}
}

func (w *OffsetNotify) Set(nextOffset int64) {
	// acquire current barrier
	b, ok := <-w.barrier
	if !ok {
		// already closed
		return
	}

	// set the new offset
	if w.nextOffset.Load() < nextOffset {
		w.nextOffset.Store(nextOffset)
	}

	// close the current barrier, e.g. broadcasting update
	close(b)

	// create new barrier
	w.barrier <- make(chan struct{})
}

func (w *OffsetNotify) Close() error {
	// acquire current barrier
	b, ok := <-w.barrier
	if !ok {
		// already closed, return an error
		return fmt.Errorf("[klevdb.OffsetNotify.Close] aquire barrier: %w", ErrOffsetNotifyClosed)
	}

	// close the current barrier, e.g. broadcasting update
	close(b)

	// close the barrier channel, completing process
	close(w.barrier)

	return nil
}
