package compact

import (
	"context"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb"
)

// FindDeletes returns a set of offsets for messages with
// nil value for a given key, before a given time.
//
// Messages that have a nil value are considered deletes
// for this key, and therefore eligible for deletion.
func FindDeletes(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, error) {
	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, err
	}

	var keyOffset = art.New()
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

			// we've seen this previously, we can delete only the first instance
			if _, ok := keyOffset.Search(msg.Key); ok {
				continue
			}

			// not seen it (first instance) whithout value (e.g. delete)
			if msg.Value == nil {
				offsets[msg.Offset] = struct{}{}
			}

			// add it to the set of seen keys, so later instances are not deleted
			keyOffset.Insert(msg.Key, msg.Offset)
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

// Deletes tries to remove messages with nil value before given time.
// It will not remove messages for keys it sees before that offset.
//
// This is similar to removing keys, which were deleted (e.g. value set to nil)
// and are therfore no longer relevant/active.
//
// returns the offsets it deleted and the amount of storage freed
func Deletes(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	offsets, err := FindDeletes(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}

// DeletesMulti is similar to Deletes, but will try to remove messages from multiple segments
func DeletesMulti(ctx context.Context, l klevdb.Log, before time.Time, backoff klevdb.DeleteMultiBackoff) (map[int64]struct{}, int64, error) {
	offsets, err := FindDeletes(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return klevdb.DeleteMulti(ctx, l, offsets, backoff)
}
