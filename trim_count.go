package klevdb

import (
	"context"
)

// FindByCount returns a set of offsets for messages that when removed will keep the number of messages in the log under max
func FindByCount(ctx context.Context, l Log, max int) (map[int64]struct{}, error) {
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
	for offset := OffsetOldest; offset < maxOffset && toRemove > 0; {
		nextOffset, msgs, err := l.Consume(offset, 32)
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

// TrimByCount tries to remove messages to keep the number of messages in the log under max count.
//
// returns the messages it deleted and the amount of storage freed
func TrimByCount(ctx context.Context, l Log, max int) ([]Message, int64, error) {
	offsets, err := FindByCount(ctx, l, max)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}

// TrimByCountMulti is similar to [TrimByCount], but will try to remove messages from multiple segments
func TrimByCountMulti(ctx context.Context, l Log, max int, backoff DeleteMultiBackoff) ([]Message, int64, error) {
	offsets, err := FindByCount(ctx, l, max)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMulti(ctx, l, offsets, backoff)
}

// TrimByCountMultiOffsets is similar to [TrimByCountMulti], but only returns the deleted offsets
func TrimByCountMultiOffsets(ctx context.Context, l Log, max int, backoff DeleteMultiBackoff) (map[int64]struct{}, int64, error) {
	offsets, err := FindByCount(ctx, l, max)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMultiOffsets(ctx, l, offsets, backoff)
}
