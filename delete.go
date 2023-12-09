package klevdb

import (
	"context"
	"maps"
)

type DeleteMultiBackoff func(context.Context) error

// DeleteMulti tries to delete all messages with offsets
//   from the log and returns the amount of storage deleted
// If error is encountered, it will return the deleted offsets
//   and size, together with the error
// DeleteMultiBackoff is called on each iteration to give
//   others a chanse to work with the log, while being deleted
func DeleteMulti(ctx context.Context, l Log, offsets map[int64]struct{}, backoff DeleteMultiBackoff) (map[int64]struct{}, int64, error) {
	var deletedOffsets = map[int64]struct{}{}
	var deletedSize int64

	for len(offsets) > 0 {
		deleted, size, err := l.Delete(offsets)
		switch {
		case err != nil:
			return deletedOffsets, deletedSize, err
		case len(deleted) == 0:
			return deletedOffsets, deletedSize, nil
		}

		maps.Copy(deletedOffsets, deleted)
		deletedSize += size
		maps.DeleteFunc(offsets, func(k int64, v struct{}) bool {
			_, ok := deleted[k]
			return ok
		})

		if err := backoff(ctx); err != nil {
			return deletedOffsets, deletedSize, err
		}
	}

	return deletedOffsets, deletedSize, nil
}
