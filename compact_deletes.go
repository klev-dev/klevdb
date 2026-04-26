package klevdb

import (
	"context"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"
)

// FindDeletes returns a set of offsets for messages with nil value for a given key, before a given time.
//
// Messages that have a nil value are considered deletes for this key, and therefore eligible for deletion.
func FindDeletes(ctx context.Context, l Log, before time.Time) (map[int64]struct{}, error) {
	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, err
	}

	var keyOffset = art.New()
	var offsets = map[int64]struct{}{}

SEARCH:
	for offset := OffsetOldest; offset < maxOffset; {
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

			// not seen it (first instance) without value (e.g. delete)
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

// CompactDeletes tries to remove messages with nil value before given time.
// It will not remove messages for keys it sees before that offset.
//
// This is similar to removing keys, which were deleted (e.g. value set to nil)
// and are therefore no longer relevant/active.
//
// returns the messages it deleted and the amount of storage freed
func CompactDeletes(ctx context.Context, l Log, before time.Time) ([]Message, int64, error) {
	offsets, err := FindDeletes(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}

// CompactDeletesMulti is similar to [CompactDeletes], but will try to remove messages from multiple segments
func CompactDeletesMulti(ctx context.Context, l Log, before time.Time, backoff DeleteMultiBackoff) ([]Message, int64, error) {
	offsets, err := FindDeletes(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMulti(ctx, l, offsets, backoff)
}

// CompactDeletesMultiOffsets is similar to [CompactDeletesMulti], but only returns the deleted offsets
func CompactDeletesMultiOffsets(ctx context.Context, l Log, before time.Time, backoff DeleteMultiBackoff) (map[int64]struct{}, int64, error) {
	offsets, err := FindDeletes(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMultiOffsets(ctx, l, offsets, backoff)
}
