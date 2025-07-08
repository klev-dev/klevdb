package trim

import (
	"context"

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
		return nil, err
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
			return nil, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Offset >= before {
				break
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

// ByOffset tries to remove the messages at the start of the log before offset
//
// returns the offsets it deleted and the amount of storage freed
func ByOffset(ctx context.Context, l klevdb.Log, before int64) (map[int64]struct{}, int64, error) {
	offsets, err := FindByOffset(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}

// ByOffsetMulti is similar to ByOffset, but will try to remove messages from multiple segments
func ByOffsetMulti(ctx context.Context, l klevdb.Log, before int64, backoff klevdb.DeleteMultiBackoff) (map[int64]struct{}, int64, error) {
	offsets, err := FindByOffset(ctx, l, before)
	if err != nil {
		return nil, 0, err
	}
	return klevdb.DeleteMulti(ctx, l, offsets, backoff)
}
