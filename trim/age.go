package trim

import (
	"context"
	"errors"
	"fmt"
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
			return nil, fmt.Errorf("[trim.FindByAge] %s next offset, no index: %w", l, err)
		}
	case errors.Is(err, klevdb.ErrNotFound):
		// all messages are before, again use the max as a bound
		maxOffset, err = l.NextOffset()
		if err != nil {
			return nil, fmt.Errorf("[trim.FindByAge] %s next offset, not found: %w", l, err)
		}
	default:
		// something else went wrong
		return nil, fmt.Errorf("[trim.FindByAge] %s offset by time: %w", l, err)
	}

	var offsets = map[int64]struct{}{}

SEARCH:
	for offset := klevdb.OffsetOldest; offset < maxOffset; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, fmt.Errorf("[trim.FindByAge] %s consume %d: %w", l, offset, err)
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Time.After(before) {
				break SEARCH
			}

			offsets[msg.Offset] = struct{}{}
		}

		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("[trim.FindByAge] %s canceled %d: %w", l, offset, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("[trim.FindByAge] %s canceled: %w", l, err)
	}

	return offsets, nil
}

// ByAge tries to remove the messages at the start of the log before given time.
//
// returns the offsets it deleted and the amount of storage freed
func ByAge(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	offsets, err := FindByAge(ctx, l, before)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByAge] %s find: %w", l, err)
	}
	m, sz, err := l.Delete(offsets)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByAge] %s delete: %w", l, err)
	}
	return m, sz, nil
}
