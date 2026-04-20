package index

import (
	"github.com/klev-dev/klevdb/pkg/message"
)

type Item struct {
	Offset    int64
	Position  int64
	Timestamp int64
	KeyHash   uint64
}

type Params struct {
	Times bool
	Keys  bool
}

func (o Params) Size() int64 {
	sz := int64(8 + 8) // offset + position
	if o.Times {
		sz += 8
	}
	if o.Keys {
		sz += 8
	}
	return sz
}

func (o Params) NewItem(m message.Message, position int64, prevts int64) Item {
	it := Item{Offset: m.Offset, Position: position}

	if o.Times {
		// guarantee timestamp monotonic increase
		it.Timestamp = max(m.Time.UnixMicro(), prevts)
	}

	if o.Keys {
		it.KeyHash = KeyHash(m.Key)
	}

	return it
}
