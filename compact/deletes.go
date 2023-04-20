package compact

import (
	"context"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/klev-dev/klevdb"
)

// Deletes tries to remove messages with nil value before given time.
//
//	It will not remove messages for keys it sees before that offset.
//
// This is similar to removing keys, which were deleted (e.g. value set to nil)
//
//	and are therfore no longer relevant/active.
//
// returns the offsets it deleted and the amount of storage freed
func Deletes(ctx context.Context, l klevdb.Log, before time.Time) (map[int64]struct{}, int64, error) {
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

			// we've seen this previously, we can delete only the first instance
			if _, ok := keyOffset.Search(msg.Key); ok {
				continue
			}

			// not seen it (first instance) whithout value (e.g. delete)
			if msg.Value == nil {
				deleteOffsets[msg.Offset] = struct{}{}
			}

			// add it to the set of seen keys, so later instances are not deleted
			keyOffset.Insert(msg.Key, msg.Offset)
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
