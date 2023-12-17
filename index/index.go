package index

import (
	"sync/atomic"

	"github.com/klev-dev/kleverr"
)

type baseRuntime[I Item] struct {
	items      []I
	nextOffset atomic.Int64
}

func newBaseRuntime[I Item](items []I, offset int64) *baseRuntime[I] {
	r := &baseRuntime[I]{}
	if ln := len(items); ln > 0 {
		r.items = items
		offset = items[ln-1].Offset() + 1
	}
	r.nextOffset.Store(offset)
	return r
}

func (r *baseRuntime[I]) Append(items []I) int64 {
	if ln := len(items); ln > 0 {
		r.items = append(r.items, items...)
		r.nextOffset.Store(items[ln-1].Offset() + 1)
	}
	return r.nextOffset.Load()
}

func (r *baseRuntime[I]) GetNextOffset() int64 {
	return r.nextOffset.Load()
}

func (r *baseRuntime[I]) GetLastOffset() int64 {
	return r.items[len(r.items)-1].Offset()
}

func (r *baseRuntime[I]) Consume(offset int64) (int64, int64, error) {
	return Consume(r.items, offset)
}

func (r *baseRuntime[I]) Get(offset int64) (int64, error) {
	return Get(r.items, offset)
}

func (r *baseRuntime[I]) Keys(_ []byte) ([]int64, error) {
	return nil, kleverr.Newf("%w by key", ErrNoIndex)
}

func (r *baseRuntime[I]) Time(_ int64) (int64, error) {
	return -1, kleverr.Newf("%w by time", ErrNoIndex)
}

func (r *baseRuntime[I]) Len() int {
	return len(r.items)
}
