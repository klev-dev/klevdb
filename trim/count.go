package trim

import (
	"context"

	"github.com/klev-dev/klevdb"
)

var countOptions = &klevdb.ConsumeOptions{MaxMessages: 32}

// FindByCount returns a set of offsets for messages that when
// removed will keep number of the messages in the log less then max
func FindByCount(ctx context.Context, l klevdb.Log, max int) (map[int64]struct{}, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, err
	case stats.Messages <= max:
		return nil, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, err
	}

	var offsets = map[int64]struct{}{}

	toRemove := stats.Messages - max
	for offset := klevdb.OffsetOldest; offset < maxOffset && toRemove > 0; {
		nextOffset, msgs, err := l.Consume(offset, countOptions)
		if err != nil {
			return nil, err
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
			return nil, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
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
		return nil, 0, err
	}
	return l.Delete(offsets)
}
