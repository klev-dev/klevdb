package index

import (
	"errors"

	"github.com/klev-dev/klevdb/message"
)

var ErrNoIndex = errors.New("no index")

type IndexItem interface {
	Offset() int64
	Position() int64
	Equal(other IndexItem) bool
}

type IndexContext any

type Index[I IndexItem, C IndexContext, S IndexStore] interface {
	Size() int64

	NewContext() C
	Context(o I) C

	New(msg message.Message, position int64, ctx C) (I, C, error)
	Read(buff []byte) (I, error)
	Write(o I, buff []byte) error

	NewStore(items []I) S
	Append(s S, items []I)
}

type IndexStore interface {
	GetLastOffset() int64

	Consume(offset int64) (int64, int64, error)
	Get(offset int64) (int64, error)

	Keys(hash []byte) ([]int64, error)
	Time(ts int64) (int64, error)

	Len() int
}
