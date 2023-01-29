package compact

import (
	"context"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/klev-dev/klevdb"
)

// Updates tries to remove messages before given time that are repeated
//   further in the log leaving only the last message for a given key.
// This is similar to removing the old value updates,
//   leaving only the current value (last update) for a key.
// returns the offsets it deleted and the amount of storage freed
func Updates(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
	maxOffset, err := l.NextOffset()
	if err != nil {
		return nil, 0, err
	}

	var keyOffset = art.New()
	var deleteOffsets = map[int64]struct{}{}
	var ontime = true

	for offset := klevdb.OffsetOldest; offset < maxOffset && ontime; {
		nextOffset, msgs, err := l.Consume(offset, 32)
		if err != nil {
			return nil, 0, err
		}
		offset = nextOffset

		for _, msg := range msgs {
			if msg.Time.After(before) {
				ontime = false
				break
			}

			if prevMsgOffset, ok := keyOffset.Insert(msg.Key, msg.Offset); ok {
				deleteOffsets[prevMsgOffset.(int64)] = struct{}{}
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
