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

func (o NoItem) Equal(other IndexItem) bool {
	oit := other.(NoItem)
	return o == oit
}

type NoIndex struct {
	zero struct{}
}

var _ Index[NoItem, struct{}, NoIndexStore] = NoIndex{}

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

func (ix NoIndex) NewStore() NoIndexStore {
	return NoIndexStore{}
}

type NoIndexStore struct {
}
