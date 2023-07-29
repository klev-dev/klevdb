package trim

import (
	"context"

	"github.com/klev-dev/klevdb"
)

func ByCount(ctx context.Context, l klevdb.Log, max int) (map[int64]struct{}, int64, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, 0, err
	case stats.Messages <= max:
		return nil, 0, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, 0, err
	}

	var deleteOffsets = map[int64]struct{}{}

	toRemove := stats.Messages - max
	for offset := klevdb.OffsetOldest; offset < maxOffset && toRemove > 0; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, 0, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			deleteOffsets[msg.Offset] = struct{}{}
			toRemove--

			if toRemove == 0 {
				break
			}
		}

		if err := ctx.Err(); err != nil {
			return nil, 0, err
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, 0, err
	}

	return l.Delete(deleteOffsets)
}
