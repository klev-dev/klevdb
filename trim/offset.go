package trim

import (
	"context"

	"github.com/klev-dev/klevdb"
)

// ByOffset tries to remove the messages at the start of the log before offset
// returns the offsets it deleted and the amount of storage freed
func ByOffset(ctx context.Context, l klevdb.Log, before int64) (map[int64]struct{}, int64, error) {
	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, 0, err
	}
	if maxOffset > before {
		maxOffset = before
	}

	var deleteOffsets = map[int64]struct{}{}
	for offset := klevdb.OffsetOldest; offset < maxOffset; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, 0, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Offset >= before {
				break
			}
			deleteOffsets[msg.Offset] = struct{}{}
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
