package index

import (
	"encoding/binary"
	"hash/fnv"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
)

type PKeyItem struct {
	offset   int64
	position int64
	keyHash  [8]byte
}

func (o PKeyItem) Offset() int64 {
	return o.offset
}

func (o PKeyItem) Position() int64 {
	return o.position
}

type PKeyIndex struct {
	zero struct{}
}

var _ Index[PKeyItem, struct{}, *PKeyIndexRuntime] = PKeyIndex{}

func (ix PKeyIndex) Size() int64 {
	return 8 + 8 + 8
}

func (ix PKeyIndex) NewState() struct{} {
	return ix.zero
}

func (ix PKeyIndex) State(_ PKeyItem) struct{} {
	return ix.zero
}

func (ix PKeyIndex) New(m message.Message, position int64, _ struct{}) (PKeyItem, struct{}, error) {
	item := PKeyItem{
		offset:   m.Offset,
		position: position,
	}

	hasher := fnv.New64a()
	hasher.Write(m.Key)
	hasher.Sum(item.keyHash[:0])

	return item, ix.zero, nil
}

func (ix PKeyIndex) Read(buff []byte) (PKeyItem, error) {
	item := PKeyItem{
		offset:   int64(binary.BigEndian.Uint64(buff[0:])),
		position: int64(binary.BigEndian.Uint64(buff[8:])),
	}
	copy(item.keyHash[:], buff[16:])
	return item, nil
}

func (ix PKeyIndex) Write(item PKeyItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(item.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(item.position))
	copy(buff[16:], item.keyHash[:])

	return nil
}

func (ix PKeyIndex) NewRuntime(items []PKeyItem, nextOffset int64, _ struct{}) *PKeyIndexRuntime {
	r := &PKeyIndexRuntime{
		OffsetRuntimeT: newOffsetRuntime(items, nextOffset),
		keys:           art.New(),
	}

	if len(items) > 0 {
		PAppendKeys(r.keys, items)
	}

	return r
}

func (ix PKeyIndex) Append(r *PKeyIndexRuntime, items []PKeyItem) int64 {
	if len(items) > 0 {
		PAppendKeys(r.keys, items)
	}
	return r.Append(items)
}

func (ix PKeyIndex) Next(r *PKeyIndexRuntime) (int64, struct{}) {
	return r.nextOffset.Load(), ix.zero
}

func (ix PKeyIndex) Equal(l, r PKeyItem) bool {
	return l == r
}

type PKeyIndexRuntime struct {
	*OffsetRuntimeT[PKeyItem]
	keys art.Tree
}

func (r *PKeyIndexRuntime) Keys(hash []byte) ([]int64, error) {
	return Keys(r.keys, hash)
}

func PAppendKeys(keys art.Tree, items []PKeyItem) {
	for _, item := range items {
		var positions []int64
		if v, found := keys.Search(item.keyHash[:]); found {
			positions = v.([]int64)
		}
		positions = append(positions, item.Position())

		keys.Insert(item.keyHash[:], positions)
	}
}
