package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
)

type Item struct {
	offset    int64
	position  int64
	timestamp int64
	keyHash   uint64
}

var _ IndexItem = Item{}

func (o Item) Offset() int64 {
	return o.offset
}

func (o Item) Position() int64 {
	return o.position
}

func (o Item) Timestamp() int64 {
	return o.timestamp
}

func (o Item) KeyHash() uint64 {
	return o.keyHash
}

func (o Item) Equal(other IndexItem) bool {
	oit := other.(Item)
	return o == oit
}

type Params struct {
	times     bool
	keys      bool
	keyOffset int
}

var _ Index[Item, int64] = Params{}

func NewParams(times bool, keys bool) Params {
	keyOffset := 8 + 8
	if times {
		keyOffset += 8
	}
	return Params{
		times:     times,
		keys:      keys,
		keyOffset: keyOffset,
	}
}

func (p Params) Size() int64 {
	sz := int64(8 + 8) // offset + position
	if p.times {
		sz += 8
	}
	if p.keys {
		sz += 8
	}
	return sz
}

func (p Params) NewContext() int64 {
	return 0
}

func (p Params) New(m message.Message, position int64, ts int64) (Item, int64, error) {
	it := Item{offset: m.Offset, position: position}

	if p.times {
		it.timestamp = m.Time.UnixMicro()
		// guarantee timestamp monotonic increase
		if it.timestamp < ts {
			it.timestamp = ts
		}
	}

	if p.keys {
		it.keyHash = KeyHash(m.Key)
	}

	return it, it.timestamp, nil
}

func (p Params) Read(buff []byte) (Item, error) {
	var o Item

	o.offset = int64(binary.BigEndian.Uint64(buff[0:]))
	o.position = int64(binary.BigEndian.Uint64(buff[8:]))

	if p.times {
		o.timestamp = int64(binary.BigEndian.Uint64(buff[16:]))
	}

	if p.keys {
		o.keyHash = binary.BigEndian.Uint64(buff[p.keyOffset:])
	}

	return o, nil
}

func (p Params) Write(o Item, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.position))

	if p.times {
		binary.BigEndian.PutUint64(buff[16:], uint64(o.timestamp))
	}

	if p.keys {
		binary.BigEndian.PutUint64(buff[p.keyOffset:], o.keyHash)
	}

	return nil
}
