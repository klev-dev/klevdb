package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

type TimeItem struct {
	offset    int64
	position  int64
	timestamp int64
}

func (o TimeItem) Offset() int64 {
	return o.offset
}

func (o TimeItem) Position() int64 {
	return o.position
}

func (o TimeItem) Timestamp() int64 {
	return o.timestamp
}

type TimeIndex struct {
}

var _ Index[TimeItem, int64, *TimeIndexStore] = TimeIndex{}

func (ix TimeIndex) Size() int64 {
	return 8 + 8 + 8
}

func (ix TimeIndex) NewContext() int64 {
	return 0
}

func (ix TimeIndex) Context(o TimeItem) int64 {
	return o.timestamp
}

func (ix TimeIndex) New(m message.Message, position int64, ts int64) (TimeItem, int64, error) {
	it := TimeItem{
		offset:    m.Offset,
		position:  position,
		timestamp: m.Time.UnixMicro(),
	}

	if it.timestamp < ts {
		it.timestamp = ts
	}

	return it, it.timestamp, nil
}

func (ix TimeIndex) Read(buff []byte) (TimeItem, error) {
	return TimeItem{
		offset:    int64(binary.BigEndian.Uint64(buff[0:])),
		position:  int64(binary.BigEndian.Uint64(buff[8:])),
		timestamp: int64(binary.BigEndian.Uint64(buff[16:])),
	}, nil
}

func (ix TimeIndex) Write(o TimeItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.position))
	binary.BigEndian.PutUint64(buff[16:], uint64(o.timestamp))

	return nil
}

func (ix TimeIndex) NewStore(items []TimeItem) *TimeIndexStore {
	return &TimeIndexStore{
		items: items,
	}
}

func (ix TimeIndex) Append(s *TimeIndexStore, items []TimeItem) {
	s.items = append(s.items, items...)
}

func (ix TimeIndex) Equal(l, r TimeItem) bool {
	return l == r
}

type TimeIndexStore struct {
	items []TimeItem
}

func (s TimeIndexStore) GetLastOffset() int64 {
	return s.items[len(s.items)-1].Offset()
}

func (s TimeIndexStore) Consume(offset int64) (int64, int64, error) {
	return Consume(s.items, offset)
}

func (s TimeIndexStore) Get(offset int64) (int64, error) {
	return Get(s.items, offset)
}

func (s TimeIndexStore) Keys(_ []byte) ([]int64, error) {
	return nil, kleverr.Newf("%w by key", ErrNoIndex)
}

func (s TimeIndexStore) Time(ts int64) (int64, error) {
	return Time(s.items, ts)
}

func (s TimeIndexStore) Len() int {
	return len(s.items)
}
