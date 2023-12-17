package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
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

func (o KeyItem) Equal(other IndexItem) bool {
	oit := other.(KeyItem)
	return o == oit
}

type KeyIndex struct {
	zero struct{}
}

var _ Index[KeyItem, struct{}, *KeyIndexStore] = KeyIndex{}

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

func (ix KeyIndex) NewStore(items []KeyItem) *KeyIndexStore {
	keys := art.New()
	AppendKeys(keys, items)
	return &KeyIndexStore{
		items: items,
		keys:  keys,
	}
}

func (ix KeyIndex) Append(s *KeyIndexStore, items []KeyItem) {
	s.items = append(s.items, items...)
	AppendKeys(s.keys, items)
}

type KeyIndexStore struct {
	items []KeyItem
	keys  art.Tree
}

func (s KeyIndexStore) GetLastOffset() int64 {
	return s.items[len(s.items)-1].Offset()
}

func (s KeyIndexStore) Consume(offset int64) (int64, int64, error) {
	return Consume(s.items, offset)
}

func (s KeyIndexStore) Get(offset int64) (int64, error) {
	return Get(s.items, offset)
}

func (s KeyIndexStore) Keys(hash []byte) ([]int64, error) {
	return Keys(s.keys, hash)
}

func (s KeyIndexStore) Time(_ int64) (int64, error) {
	return -1, kleverr.Newf("%w by time", ErrNoIndex)
}

func (s KeyIndexStore) Len() int {
	return len(s.items)
}
