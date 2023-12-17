package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

type NoItem struct {
	offset   int64
	position int64
}

func (o NoItem) Offset() int64 {
	return o.offset
}

func (o NoItem) Position() int64 {
	return o.position
}

type NoIndex struct {
	zero struct{}
}

var _ Index[NoItem, struct{}, *NoIndexStore] = NoIndex{}

func (ix NoIndex) Size() int64 {
	return 8 + 8
}

func (ix NoIndex) NewContext() struct{} {
	return ix.zero
}

func (ix NoIndex) Context(o NoItem) struct{} {
	return ix.zero
}

func (ix NoIndex) New(m message.Message, position int64, _ struct{}) (NoItem, struct{}, error) {
	return NoItem{
		offset:   m.Offset,
		position: position,
	}, ix.zero, nil
}

func (ix NoIndex) Read(buff []byte) (NoItem, error) {
	return NoItem{
		offset:   int64(binary.BigEndian.Uint64(buff[0:])),
		position: int64(binary.BigEndian.Uint64(buff[8:])),
	}, nil
}

func (ix NoIndex) Write(o NoItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.position))

	return nil
}

func (ix NoIndex) NewStore(items []NoItem) *NoIndexStore {
	return &NoIndexStore{
		items: items,
	}
}

func (ix NoIndex) Append(s *NoIndexStore, items []NoItem) {
	s.items = append(s.items, items...)
}

func (ix NoIndex) Equal(l, r NoItem) bool {
	return l == r
}

type NoIndexStore struct {
	items []NoItem
}

func (s NoIndexStore) GetLastOffset() int64 {
	return s.items[len(s.items)-1].Offset()
}

func (s NoIndexStore) Consume(offset int64) (int64, int64, error) {
	return Consume(s.items, offset)
}

func (s NoIndexStore) Get(offset int64) (int64, error) {
	return Get(s.items, offset)
}

func (s NoIndexStore) Keys(_ []byte) ([]int64, error) {
	return nil, kleverr.Newf("%w by key", ErrNoIndex)
}

func (s NoIndexStore) Time(_ int64) (int64, error) {
	return -1, kleverr.Newf("%w by time", ErrNoIndex)
}

func (s NoIndexStore) Len() int {
	return len(s.items)
}
