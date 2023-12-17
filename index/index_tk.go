package index

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
)

type TimeKeyItem struct {
	offset    int64
	position  int64
	timestamp int64
	keyHash   uint64
}

func (o TimeKeyItem) Offset() int64 {
	return o.offset
}

func (o TimeKeyItem) Position() int64 {
	return o.position
}

func (o TimeKeyItem) Timestamp() int64 {
	return o.timestamp
}

func (o TimeKeyItem) KeyHash() uint64 {
	return o.keyHash
}

type TimeKeyIndex struct {
}

var _ Index[TimeKeyItem, int64, *TimeKeyIndexRuntime] = TimeKeyIndex{}

func (ix TimeKeyIndex) Size() int64 {
	return 8 + 8 + 8 + 8
}

func (ix TimeKeyIndex) NewState() int64 {
	return 0
}

func (ix TimeKeyIndex) State(item TimeKeyItem) int64 {
	return item.timestamp
}

func (ix TimeKeyIndex) New(m message.Message, position int64, ts int64) (TimeKeyItem, int64, error) {
	it := TimeKeyItem{
		offset:    m.Offset,
		position:  position,
		timestamp: m.Time.UnixMicro(),
		keyHash:   KeyHash(m.Key),
	}

	if it.timestamp < ts {
		it.timestamp = ts
	}

	return it, it.timestamp, nil
}

func (ix TimeKeyIndex) Read(buff []byte) (TimeKeyItem, error) {
	return TimeKeyItem{
		offset:    int64(binary.BigEndian.Uint64(buff[0:])),
		position:  int64(binary.BigEndian.Uint64(buff[8:])),
		timestamp: int64(binary.BigEndian.Uint64(buff[16:])),
		keyHash:   binary.BigEndian.Uint64(buff[24:]),
	}, nil
}

func (ix TimeKeyIndex) Write(item TimeKeyItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))
	binary.BigEndian.PutUint64(buff[16:], uint64(item.timestamp))
	binary.BigEndian.PutUint64(buff[24:], item.keyHash)

	return nil
}

func (ix TimeKeyIndex) NewRuntime(items []TimeKeyItem, nextOffset int64, nextTime int64) *TimeKeyIndexRuntime {
	r := &TimeKeyIndexRuntime{
		baseRuntime: newBaseRuntime(items, nextOffset),
		keys:        art.New(),
	}

	if ln := len(items); ln > 0 {
		AppendKeys(r.keys, items)
		nextTime = items[ln-1].timestamp
	}
	r.nextTime.Store(nextTime)

	return r
}

func (ix TimeKeyIndex) Append(s *TimeKeyIndexRuntime, items []TimeKeyItem) int64 {
	if ln := len(items); ln > 0 {
		AppendKeys(s.keys, items)
		s.nextTime.Store(items[ln-1].timestamp)
	}
	return s.Append(items)
}

func (ix TimeKeyIndex) Next(s *TimeKeyIndexRuntime) (int64, int64) {
	return s.nextOffset.Load(), s.nextTime.Load()
}

func (ix TimeKeyIndex) Equal(l, r TimeKeyItem) bool {
	return l == r
}

type TimeKeyIndexRuntime struct {
	*baseRuntime[TimeKeyItem]
	nextTime atomic.Int64
	keys     art.Tree
}

func (s *TimeKeyIndexRuntime) Keys(hash []byte) ([]int64, error) {
	return Keys(s.keys, hash)
}

func (s *TimeKeyIndexRuntime) Time(ts int64) (int64, error) {
	return Time(s.items, ts)
}
