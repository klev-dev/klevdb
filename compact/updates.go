package compact

import (
	"context"
	"fmt"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb"
)

// FindUpdates returns a set of offsets for messages that have
// the same key further in the log, before a given time.
//
// Messages before the last one for a given key are considered updates
// that are no longer relevant, and therefore are eligible for deletion.
func FindUpdates(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, error) {
	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[compact.FindUpdates] %s next offset: %w", l, err)
	}

	var keyOffset = art.New()
	var offsets = map[int64]struct{}{}

SEARCH:
	for offset := klevdb.OffsetOldest; offset < maxOffset; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, fmt.Errorf("[compact.FindUpdates] %s consume %d: %w", l, offset, err)
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Time.After(before) {
				break SEARCH
			}

			if prevMsgOffset, ok := keyOffset.Insert(msg.Key, msg.Offset); ok {
				offsets[prevMsgOffset.(int64)] = struct{}{}
			}
		}

		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("[compact.FindUpdates] %s canceled %d: %w", l, offset, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("[compact.FindUpdates] %s canceled: %w", l, err)
	}

	return offsets, nil
}

// Updates tries to remove messages before given time that are repeated
// further in the log leaving only the last message for a given key.
//
// This is similar to removing the old value updates,
// leaving only the current value (last update) for a key.
//
// returns the offsets it deleted and the amount of storage freed
func Updates(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	offsets, err := FindUpdates(ctx, l, before)
	if err != nil {
		return nil, 0, fmt.Errorf("[compact.Updates] %s find: %w", l, err)
	}
	m, sz, err := l.Delete(offsets)
	if err != nil {
		return nil, 0, fmt.Errorf("[compact.Updates] %s delete: %w", l, err)
	}
	return m, sz, nil
}
