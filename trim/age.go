package trim

import (
	"context"
	"errors"
	"time"

	"github.com/klev-dev/klevdb"
)

// ByAge tries to remove the messages at the start of the log before given time
// returns the offsets it deleted and the amount of storage freed
func ByAge(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	maxOffset, _, err := l.OffsetByTime(before)
	switch {
	case err == nil:
		// we've found the max offset, start collecting offsets to delete
		break
	case errors.Is(err, klevdb.ErrNoIndex):
		// this log is not indexed by time, use the max as a bound
		maxOffset, err = l.NextOffset()
		if err != nil {
			return nil, 0, err
		}
	case errors.Is(err, klevdb.ErrNotFound):
		// all messages are before, again use the max as a bound
		maxOffset, err = l.NextOffset()
		if err != nil {
			return nil, 0, err
		}
	default:
		// something else went wrong
		if err != nil {
			return nil, 0, err
		}
	}

	var deleteOffsets = map[int64]struct{}{}
	var ontime = true

	for offset := klevdb.OffsetOldest; offset < maxOffset && ontime; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, 0, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Time.After(before) {
				ontime = false
				break
			}

			deleteOffsets[msg.Offset] = struct{}{}
		}

		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	return l.Delete(deleteOffsets)
}
