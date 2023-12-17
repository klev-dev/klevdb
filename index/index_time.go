package index

import (
	"encoding/binary"

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
	return &TimeIndexRuntime{
		baseRuntime: baseRuntime[TimeItem]{
			items:      items,
			nextOffset: nextOffset,
		},
		nextTime: nextTime,
	}
}

func (ix TimeIndex) Append(s *TimeIndexRuntime, items []TimeItem) {
	s.items = append(s.items, items...)
}

func (ix TimeIndex) Next(s *TimeIndexRuntime) (int64, int64) {
	return s.nextOffset, s.nextTime
}

func (ix TimeIndex) Equal(l, r TimeItem) bool {
	return l == r
}

type TimeIndexRuntime struct {
	baseRuntime[TimeItem]
	nextTime int64
}

func (s TimeIndexRuntime) Time(ts int64) (int64, error) {
	return Time(s.items, ts)
}
