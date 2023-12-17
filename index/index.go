package index

import "github.com/klev-dev/kleverr"

type baseRuntime[I Item] struct {
	items      []I
	nextOffset int64
}

func (s baseRuntime[I]) GetLastOffset() int64 {
	return s.items[len(s.items)-1].Offset()
}

func (s baseRuntime[I]) Consume(offset int64) (int64, int64, error) {
	return Consume(s.items, offset)
}

func (s baseRuntime[I]) Get(offset int64) (int64, error) {
	return Get(s.items, offset)
}

func (s baseRuntime[I]) Keys(_ []byte) ([]int64, error) {
	return nil, kleverr.Newf("%w by key", ErrNoIndex)
}

func (s baseRuntime[I]) Time(_ int64) (int64, error) {
	return -1, kleverr.Newf("%w by time", ErrNoIndex)
}

func (s baseRuntime[I]) Len() int {
	return len(s.items)
}
