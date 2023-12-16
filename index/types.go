package index

import "github.com/klev-dev/klevdb/message"

type IndexItem interface {
	Timestamp() int64 // TODO get rid of this
	Equal(other IndexItem) bool
}

type Index[I IndexItem] interface {
	Size() int64

	New(msg message.Message, position int64, prevTs int64) (I, error)
	Read(buff []byte) (I, error)
	Write(o I, buff []byte) error
}
