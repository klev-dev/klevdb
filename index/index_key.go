package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
)

type KeyItem struct {
	offset   int64
	position int64
	keyHash  uint64
}

func (o KeyItem) Offset() int64 {
	return o.offset
}

func (o KeyItem) Position() int64 {
	return o.position
}

func (o KeyItem) KeyHash() uint64 {
	return o.keyHash
}

func (o KeyItem) Equal(other IndexItem) bool {
	oit := other.(KeyItem)
	return o == oit
}

type KeyIndex struct {
	zero struct{}
}

var _ Index[KeyItem, struct{}] = KeyIndex{}

func (ix KeyIndex) Size() int64 {
	return 8 + 8 + 8
}

func (ix KeyIndex) NewContext() struct{} {
	return ix.zero
}

func (ix KeyIndex) Context(o KeyItem) struct{} {
	return ix.zero
}

func (ix KeyIndex) New(m message.Message, position int64, _ struct{}) (KeyItem, struct{}, error) {
	return KeyItem{
		offset:   m.Offset,
		position: position,
		keyHash:  KeyHash(m.Key),
	}, ix.zero, nil
}

func (ix KeyIndex) Read(buff []byte) (KeyItem, error) {
	return KeyItem{
		offset:   int64(binary.BigEndian.Uint64(buff[0:])),
		position: int64(binary.BigEndian.Uint64(buff[8:])),
		keyHash:  binary.BigEndian.Uint64(buff[16:]),
	}, nil
}

func (ix KeyIndex) Write(o KeyItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.position))
	binary.BigEndian.PutUint64(buff[16:], o.keyHash)

	return nil
}
