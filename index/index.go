package index

import (
	"encoding/binary"

	"github.com/klev-dev/klevdb/message"
)

type Item struct {
	Offset    int64
	Position  int64
	Timestamp int64
	KeyHash   uint64
}

var _ IndexItem = Item{}

type Params struct {
	times     bool
	keys      bool
	keyOffset int
}

var _ Index[Item] = Params{}

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

func (p Params) New(m message.Message, position int64, prevts int64) (Item, error) {
	it := Item{Offset: m.Offset, Position: position}

	if p.times {
		it.Timestamp = m.Time.UnixMicro()
		// guarantee timestamp monotonic increase
		if it.Timestamp < prevts {
			it.Timestamp = prevts
		}
	}

	if p.keys {
		it.KeyHash = KeyHash(m.Key)
	}

	return it, nil
}

func (p Params) Read(buff []byte) (Item, error) {
	var o Item

	o.Offset = int64(binary.BigEndian.Uint64(buff[0:]))
	o.Position = int64(binary.BigEndian.Uint64(buff[8:]))

	if p.times {
		o.Timestamp = int64(binary.BigEndian.Uint64(buff[16:]))
	}

	if p.keys {
		o.KeyHash = binary.BigEndian.Uint64(buff[p.keyOffset:])
	}

	return o, nil
}

func (p Params) Write(o Item, buff []byte) error {
	binary.BigEndian.PutUint64(buff[0:], uint64(o.Offset))
	binary.BigEndian.PutUint64(buff[8:], uint64(o.Position))

	if p.times {
		binary.BigEndian.PutUint64(buff[16:], uint64(o.Timestamp))
	}

	if p.keys {
		binary.BigEndian.PutUint64(buff[p.keyOffset:], o.KeyHash)
	}

	return nil
}
