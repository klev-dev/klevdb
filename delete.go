package klevdb

import (
	"context"
	"time"

	"golang.org/x/exp/maps"
)

// DeleteMultiBackoff is call on each iteration of [DeleteMulti] to give applications
// opportunity to not overload the target log with deletes
type DeleteMultiBackoff func(context.Context) error

// DeleteMultiWithWait returns a backoff func that sleeps/waits
// for a certain duration. If context is canceled while executing
// it returns the associated error
func DeleteMultiWithWait(d time.Duration) DeleteMultiBackoff {
	return func(ctx context.Context) error {
		select {
		case <-time.After(d):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// DeleteMulti tries to delete all messages with offsets
// from the log and returns the amount of storage deleted
//
// If error is encountered, it will return the deleted offsets
// and size, together with the error
//
// [DeleteMultiBackoff] is called on each iteration to give
// others a chanse to work with the log, while being deleted
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
