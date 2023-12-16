package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
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

func (o TimeKeyItem) Equal(other IndexItem) bool {
	oit := other.(TimeKeyItem)
	return o == oit
}

type TimeKeyIndex struct {
}

var _ Index[TimeKeyItem, int64] = TimeKeyIndex{}

func (ix TimeKeyIndex) Size() int64 {
	return 8 + 8 + 8 + 8
}

func (ix TimeKeyIndex) NewContext() int64 {
	return 0
}

func (ix TimeKeyIndex) Context(o TimeKeyItem) int64 {
	return o.timestamp
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

func (ix TimeKeyIndex) Write(o TimeKeyItem, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.position))
	binary.BigEndian.PutUint64(buff[16:], uint64(o.timestamp))
	binary.BigEndian.PutUint64(buff[24:], o.keyHash)

	return nil
}
