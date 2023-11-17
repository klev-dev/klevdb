package trim

import (
	"context"
	"errors"
	"time"

	"github.com/klev-dev/klevdb"
)

// FindByAge returns a set of offsets for messages that are
// at the start of the log and before given time.
func FindByAge(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, error) {
	maxOffset, _, err := l.OffsetByTime(before)
	switch {
	case err == nil:
		// we've found the max offset, start collecting offsets to delete
		break
	case errors.Is(err, klevdb.ErrNoIndex):
		// this log is not indexed by time, use the max as a bound
		maxOffset, err = l.NextOffset()
		if err != nil {
			return nil, err
		}
	case errors.Is(err, klevdb.ErrNotFound):
		// all messages are before, again use the max as a bound
		maxOffset, err = l.NextOffset()
		if err != nil {
			return nil, err
		}
	default:
		// something else went wrong
		if err != nil {
			return nil, err
		}
	}

	var offsets = map[int64]struct{}{}

SEARCH:
	for offset := klevdb.OffsetOldest; offset < maxOffset; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Time.After(before) {
				break SEARCH
			}

			offsets[msg.Offset] = struct{}{}
		}

		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}

	return offsets, nil
}

// ByAge tries to remove the messages at the start of the log before given time.
//
// returns the offsets it deleted and the amount of storage freed
func ByAge(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	offsets, err := FindByAge(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}
