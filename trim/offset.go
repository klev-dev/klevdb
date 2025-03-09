package trim

import (
	"context"
	"fmt"

	"github.com/klev-dev/klevdb"
	"github.com/klev-dev/klevdb/message"
)

// FindByOffset returns a set of offsets for messages that
// offset is before a given offset
func FindByOffset(ctx context.Context, l klevdb.Log, before int64) (map[int64]struct{}, error) {
	if before == message.OffsetOldest {
		return map[int64]struct{}{}, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[trim.FindByOffset] %s next offset: %w", l, err)
	}
	if before == message.OffsetNewest {
		before = maxOffset
	} else if maxOffset > before {
		maxOffset = before
	}

	var offsets = map[int64]struct{}{}
	for offset := klevdb.OffsetOldest; offset < maxOffset; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, fmt.Errorf("[trim.FindByOffset] %s consume %d: %w", l, offset, err)
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Offset >= before {
				break
			}
			offsets[msg.Offset] = struct{}{}
		}

		if err := ctx.Err(); err != nil {
			return nil, fmt.Errorf("[trim.FindByOffset] %s canceled %d: %w", l, offset, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("[trim.FindByOffset] %s canceled: %w", l, err)
	}

	return offsets, nil
}

// ByOffset tries to remove the messages at the start of the log before offset
//
// returns the offsets it deleted and the amount of storage freed
func ByOffset(ctx context.Context, l klevdb.Log, before int64) (map[int64]struct{}, int64, error) {
	offsets, err := FindByOffset(ctx, l, before)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByOffset] %s find: %w", l, err)
	}
	m, sz, err := l.Delete(offsets)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.ByOffset] %s delete: %w", l, err)
	}
	return m, sz, nil
}
