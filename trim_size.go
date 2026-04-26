package klevdb

import (
	"context"
)

// FindBySize returns a set of offsets for messages that if deleted will decrease the log size to sz
func FindBySize(ctx context.Context, l Log, sz int64) (map[int64]struct{}, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, err
	case stats.Size < sz:
		return nil, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, err
	}

	var offsets = map[int64]struct{}{}

	total := stats.Size
	for offset := OffsetOldest; offset < maxOffset && total >= sz; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			offsets[msg.Offset] = struct{}{}
			total -= l.Size(msg)

			if total < sz {
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

// TrimBySize tries to remove messages until log size is less than sz
//
// returns the messages it deleted and the amount of storage freed
func TrimBySize(ctx context.Context, l Log, sz int64) ([]Message, int64, error) {
	offsets, err := FindBySize(ctx, l, sz)
	if err != nil {
		return nil, 0, err
	}
	return l.Delete(offsets)
}

// TrimBySizeMulti is similar to [TrimBySize], but will try to remove messages from multiple segments
func TrimBySizeMulti(ctx context.Context, l Log, sz int64, backoff DeleteMultiBackoff) ([]Message, int64, error) {
	offsets, err := FindBySize(ctx, l, sz)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMulti(ctx, l, offsets, backoff)
}

// TrimBySizeMultiOffsets is similar to [TrimBySizeMulti], but only returns the deleted offsets
func TrimBySizeMultiOffsets(ctx context.Context, l Log, sz int64, backoff DeleteMultiBackoff) ([]Message, int64, error) {
	offsets, err := FindBySize(ctx, l, sz)
	if err != nil {
		return nil, 0, err
	}
	return DeleteMulti(ctx, l, offsets, backoff)
}
