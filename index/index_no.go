package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
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

var _ Index[NoItem, struct{}, *NoIndexRuntime] = NoIndex{}

func (ix NoIndex) Size() int64 {
	return 8 + 8
}

func (ix NoIndex) NewState() struct{} {
	return ix.zero
}

func (ix NoIndex) State(_ NoItem) struct{} {
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

func (ix NoIndex) Write(item NoItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))

	return nil
}

func (ix NoIndex) NewRuntime(items []NoItem, nextOffset int64, _ struct{}) *NoIndexRuntime {
	return &NoIndexRuntime{
		baseRuntime: baseRuntime[NoItem]{
			items:      items,
			nextOffset: nextOffset,
		},
	}
}

func (ix NoIndex) Append(s *NoIndexRuntime, items []NoItem) {
	s.items = append(s.items, items...)
}

func (ix NoIndex) Next(s *NoIndexRuntime) (int64, struct{}) {
	return s.nextOffset, ix.zero
}

func (ix NoIndex) Equal(l, r NoItem) bool {
	return l == r
}

type NoIndexRuntime struct {
	baseRuntime[NoItem]
}
