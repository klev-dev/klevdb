package index

import "github.com/klev-dev/klevdb/message"

type IndexItem interface {
	Offset() int64
	Position() int64
	Equal(other IndexItem) bool
}

type IndexContext any

type Index[I IndexItem, C IndexContext] interface {
	Size() int64

	NewContext() C
	Context(o I) C
	New(msg message.Message, position int64, ctx C) (I, C, error)

	Read(buff []byte) (I, error)
	Write(o I, buff []byte) error
}
