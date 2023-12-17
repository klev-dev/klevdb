package index

import (
	"errors"

	"github.com/klev-dev/klevdb/message"
)

var ErrNoIndex = errors.New("no index")

type Item interface {
	Offset() int64
	Position() int64
}

type State any

type Index[I Item, S State, R Runtime] interface {
	Size() int64

	NewState() S
	State(item I) S

	New(msg message.Message, position int64, state S) (item I, nextState S, err error)
	Read(buff []byte) (item I, err error)
	Write(item I, buff []byte) error

	NewRuntime(items []I, nextOffset int64, nextState S) R
	Append(run R, items []I) int64
	Next(run R) (nextOffset int64, nextState S)

	Equal(l, r I) bool
}

type Runtime interface {
	GetNextOffset() int64
	GetLastOffset() int64

	Consume(offset int64) (int64, int64, error)
	Get(offset int64) (int64, error)

	Keys(hash []byte) ([]int64, error)
	Time(ts int64) (int64, error)

	Len() int
}
