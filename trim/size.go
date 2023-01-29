package trim

import (
	"context"

	"github.com/klev-dev/klevdb"
)

// BySize tries to remove messages until log size is less then sz
// returns the offsets it deleted and the amount of storage freed
func BySize(ctx context.Context, l klevdb.Log, sz int64) (map[int64]struct{}, int64, error) {
	stats, err := l.Stat()
	switch {
	case err != nil:
		return nil, 0, err
	case stats.Size < sz:
		return nil, 0, nil
	}

	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, 0, err
	}

	var deleteOffsets = map[int64]struct{}{}

	total := stats.Size
	for offset := klevdb.OffsetOldest; offset < maxOffset && total >= sz; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, 0, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			deleteOffsets[msg.Offset] = struct{}{}
			total -= l.Size(msg)

			if total < sz {
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
