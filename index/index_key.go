package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
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

type KeyIndex struct {
	zero struct{}
}

var _ Index[KeyItem, struct{}, *KeyIndexRuntime] = KeyIndex{}

func (ix KeyIndex) Size() int64 {
	return 8 + 8 + 8
}

func (ix KeyIndex) NewState() struct{} {
	return ix.zero
}

func (ix KeyIndex) State(_ KeyItem) struct{} {
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

func (ix KeyIndex) Write(item KeyItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))
	binary.BigEndian.PutUint64(buff[16:], item.keyHash)

	return nil
}

func (ix KeyIndex) NewRuntime(items []KeyItem, nextOffset int64, _ struct{}) *KeyIndexRuntime {
	r := &KeyIndexRuntime{
		OffsetRuntimeT: newOffsetRuntime(items, nextOffset),
		keys:           art.New(),
	}

	if len(items) > 0 {
		AppendKeys(r.keys, items)
	}

	return r
}

func (ix KeyIndex) Append(s *KeyIndexRuntime, items []KeyItem) int64 {
	if len(items) > 0 {
		AppendKeys(s.keys, items)
	}
	return s.Append(items)
}

func (ix KeyIndex) Next(s *KeyIndexRuntime) (int64, struct{}) {
	return s.nextOffset.Load(), ix.zero
}

func (ix KeyIndex) Equal(l, r KeyItem) bool {
	return l == r
}

type KeyIndexRuntime struct {
	*OffsetRuntimeT[KeyItem]
	keys art.Tree
}

func (s *KeyIndexRuntime) Keys(hash []byte) ([]int64, error) {
	return Keys(s.keys, hash)
}
