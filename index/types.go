package index

import "github.com/klev-dev/klevdb/message"

type IndexItem interface {
	Equal(other IndexItem) bool
}

type IndexContext any

type Index[I IndexItem, C IndexContext] interface {
	Size() int64

	NewContext() C
	New(msg message.Message, position int64, ctx C) (I, C, error)

	Read(buff []byte) (I, error)
	Write(o I, buff []byte) error
}
