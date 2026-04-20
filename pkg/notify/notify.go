package notify

import (
	"context"
	"errors"
	"sync/atomic"
)

var ErrOffsetNotifyClosed = errors.New("offset notify already closed")

type Offset struct {
	nextOffset atomic.Int64
	barrier    chan chan struct{}
}

func NewOffset(nextOffset int64) *Offset {
	w := &Offset{
		barrier: make(chan chan struct{}, 1),
	}

	w.nextOffset.Store(nextOffset)
	w.barrier <- make(chan struct{})

	return w
}

func (w *Offset) Wait(ctx context.Context, offset int64) error {
	// quick path, just load and check
	if w.nextOffset.Load() > offset {
		return nil
	}

	// acquire current barrier
	b, ok := <-w.barrier
	if !ok {
		// already closed, return error
		return ErrOffsetNotifyClosed
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
		return ctx.Err()
	}
}

func (w *Offset) Set(nextOffset int64) {
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

func (w *Offset) Close() error {
	// acquire current barrier
	b, ok := <-w.barrier
	if !ok {
		// already closed, return an error
		return ErrOffsetNotifyClosed
	}

	// close the current barrier, e.g. broadcasting update
	close(b)

	// close the barrier channel, completing process
	close(w.barrier)

	return nil
}
