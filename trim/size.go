package trim

import (
	"context"
	"fmt"

	"github.com/klev-dev/klevdb"
)

// FindBySize returns a set of offsets for messages that
// if deleted will decrease the log size to sz
func FindBySize(ctx context.Context, l klevdb.Log, sz int64) (map[int64]struct{}, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, fmt.Errorf("[trim.FindBySize] %s stat: %w", l, err)
	case stats.Size < sz:
		return nil, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[trim.FindBySize] %s next offset: %w", l, err)
	}

	var offsets = map[int64]struct{}{}

	total := stats.Size
	for offset := klevdb.OffsetOldest; offset < maxOffset && total >= sz; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, fmt.Errorf("[trim.FindBySize] %s consume %d: %w", l, offset, err)
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
			return nil, fmt.Errorf("[trim.FindBySize] %s canceled %d: %w", l, offset, err)
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, fmt.Errorf("[trim.FindBySize] %s canceled: %w", l, err)
	}

	return offsets, nil
}

// BySize tries to remove messages until log size is less then sz
//
// returns the offsets it deleted and the amount of storage freed
func BySize(ctx context.Context, l klevdb.Log, sz int64) (map[int64]struct{}, int64, error) {
	offsets, err := FindBySize(ctx, l, sz)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.BySize] %s find: %w", l, err)
	}
	m, sz, err := l.Delete(offsets)
	if err != nil {
		return nil, 0, fmt.Errorf("[trim.BySize] %s delete: %w", l, err)
	}
	return m, sz, nil
}
