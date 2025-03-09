package trim

import (
	"context"
	"fmt"

	"github.com/klev-dev/klevdb"
)

// FindByCount returns a set of offsets for messages that when
// removed will keep number of the messages in the log less then max
func FindByCount(ctx context.Context, l klevdb.Log, max int) (map[int64]struct{}, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, fmt.Errorf("[trim.FindByCount] %s stat: %w", l, err)
	case stats.Messages <= max:
		return nil, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[trim.FindByCount] %s next offset: %w", l, err)
	}

	var offsets = map[int64]struct{}{}

	toRemove := stats.Messages - max
	for offset := klevdb.OffsetOldest; offset < maxOffset && toRemove > 0; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, fmt.Errorf("[trim.FindByCount] %s consume %d: %w", l, offset, err)
		}
		offset = nextOffset

		for _, msg := range msgs {
			offsets[msg.Offset] = struct{}{}
			toRemove--

			if toRemove <= 0 {
				break
			}
		}

		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("[trim.FindByCount] %s canceled %d: %w", l, offset, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("[trim.FindByCount] %s canceled: %w", l, err)
	}

	return offsets, nil
}

// ByCount tries to remove messages to keep the number of messages
// in the log under max count.
//
// returns the offsets it deleted and the amount of storage freed
func ByCount(ctx context.Context, l klevdb.Log, max int) (map[int64]struct{}, int64, error) {
	offsets, err := FindByCount(ctx, l, max)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByCount] %s find: %w", l, err)
	}
	m, sz, err := l.Delete(offsets)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByCount] %s delete: %w", l, err)
	}
	return m, sz, nil
}
