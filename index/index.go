package index

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

type OffsetItem struct {
	offset   int64
	position int64
}

func (o OffsetItem) Offset() int64 {
	return o.offset
}

func (o OffsetItem) Position() int64 {
	return o.position
}

type OffsetIndex struct {
	zero struct{}
}

var _ Index[OffsetItem, struct{}, *OffsetRuntime] = OffsetIndex{}

func (ix OffsetIndex) Size() int64 {
	return 8 + 8
}

func (ix OffsetIndex) NewState() struct{} {
	return ix.zero
}

func (ix OffsetIndex) State(_ OffsetItem) struct{} {
	return ix.zero
}

func (ix OffsetIndex) New(m message.Message, position int64, _ struct{}) (OffsetItem, struct{}, error) {
	return OffsetItem{
		offset:   m.Offset,
		position: position,
	}, ix.zero, nil
}

func (ix OffsetIndex) Read(buff []byte) (OffsetItem, error) {
	return OffsetItem{
		offset:   int64(binary.BigEndian.Uint64(buff[0:])),
		position: int64(binary.BigEndian.Uint64(buff[8:])),
	}, nil
}

func (ix OffsetIndex) Write(item OffsetItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))

	return nil
}

func (ix OffsetIndex) NewRuntime(items []OffsetItem, nextOffset int64, _ struct{}) *OffsetRuntime {
	return newOffsetRuntime[OffsetItem](items, nextOffset)
}

func (ix OffsetIndex) Append(s *OffsetRuntime, items []OffsetItem) int64 {
	return s.Append(items)
}

func (ix OffsetIndex) Next(s *OffsetRuntime) (int64, struct{}) {
	return s.nextOffset.Load(), ix.zero
}

func (ix OffsetIndex) Equal(l, r OffsetItem) bool {
	return l == r
}

type OffsetRuntime = OffsetRuntimeT[OffsetItem]

type OffsetRuntimeT[I Item] struct {
	items      []I
	nextOffset atomic.Int64
}

func newOffsetRuntime[I Item](items []I, offset int64) *OffsetRuntimeT[I] {
	r := &OffsetRuntimeT[I]{}
	if ln := len(items); ln > 0 {
		r.items = items
		offset = items[ln-1].Offset() + 1
	}
	r.nextOffset.Store(offset)
	return r
}

func (r *OffsetRuntimeT[I]) Append(items []I) int64 {
	if ln := len(items); ln > 0 {
		r.items = append(r.items, items...)
		r.nextOffset.Store(items[ln-1].Offset() + 1)
	}
	return r.nextOffset.Load()
}

func (r *OffsetRuntimeT[I]) GetNextOffset() int64 {
	return r.nextOffset.Load()
}

func (r *OffsetRuntimeT[I]) GetLastOffset() int64 {
	return r.items[len(r.items)-1].Offset()
}

func (r *OffsetRuntimeT[I]) Consume(offset int64) (int64, int64, error) {
	return Consume(r.items, offset)
}

func (r *OffsetRuntimeT[I]) Get(offset int64) (int64, error) {
	return Get(r.items, offset)
}

func (r *OffsetRuntimeT[I]) Keys(_ []byte) ([]int64, error) {
	return nil, kleverr.Newf("%w by key", ErrNoIndex)
}

func (r *OffsetRuntimeT[I]) Time(_ int64) (int64, error) {
	return -1, kleverr.Newf("%w by time", ErrNoIndex)
}

func (r *OffsetRuntimeT[I]) Len() int {
	return len(r.items)
}
