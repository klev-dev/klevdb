package message

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/klev-dev/kleverr"
)

const (
	// OffsetOldest represents the smallest offset still available
	// Use it to consume all messages, starting at the first available
	OffsetOldest int64 = -2
	// OffsetNewest represents the offset that will be used for the next produce
	// Use it to consume only new messages
	OffsetNewest int64 = -1
	// OffsetInvalid is the offset returned when error is detected
	OffsetInvalid int64 = -3
)

var ErrInvalidOffset = errors.New("invalid offset")
var ErrNotFound = errors.New("not found")

func ValidateOffset(offset int64) error {
	if offset < OffsetOldest {
		return kleverr.Newf("%w: %d is not a valid offset", ErrInvalidOffset, offset)
	}
	return nil
}

type Message struct {
	Offset int64
	Time   time.Time
	Key    []byte
	Value  []byte
}

var Invalid = Message{Offset: OffsetInvalid}

func Gen(count int) []Message {
	var msgs = make([]Message, count)
	for i := range msgs {
		msgs[i] = Message{
			Time:  time.Date(2023, 1, 1, 0, 0, i, 0, time.UTC),
			Key:   []byte(fmt.Sprintf("%10d", i)),
			Value: []byte(strings.Repeat(" ", 128)),
		}
	}
	return msgs
}
