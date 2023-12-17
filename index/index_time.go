package index

import (
	"encoding/binary"
	"sync/atomic"

	"github.com/klev-dev/klevdb/message"
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

var _ Index[TimeItem, int64, *TimeIndexRuntime] = TimeIndex{}

func (ix TimeIndex) Size() int64 {
	return 8 + 8 + 8
}

func (ix TimeIndex) NewState() int64 {
	return 0
}

func (ix TimeIndex) State(item TimeItem) int64 {
	return item.timestamp
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

func (ix TimeIndex) Write(item TimeItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))
	binary.BigEndian.PutUint64(buff[16:], uint64(item.timestamp))

	return nil
}

func (ix TimeIndex) NewRuntime(items []TimeItem, nextOffset int64, nextTime int64) *TimeIndexRuntime {
	r := &TimeIndexRuntime{
		OffsetRuntimeT: newOffsetRuntime(items, nextOffset),
	}

	if ln := len(items); ln > 0 {
		nextTime = items[ln-1].timestamp
	}
	r.nextTime.Store(nextTime)

	return r
}

func (ix TimeIndex) Append(r *TimeIndexRuntime, items []TimeItem) int64 {
	if ln := len(items); ln > 0 {
		r.nextTime.Store(items[ln-1].timestamp)
	}
	return r.Append(items)
}

func (ix TimeIndex) Next(r *TimeIndexRuntime) (int64, int64) {
	return r.nextOffset.Load(), r.nextTime.Load()
}

func (ix TimeIndex) Equal(l, r TimeItem) bool {
	return l == r
}

type TimeIndexRuntime struct {
	*OffsetRuntimeT[TimeItem]
	nextTime atomic.Int64
}

func (r *TimeIndexRuntime) Time(ts int64) (int64, error) {
	return Time(r.items, ts)
}
