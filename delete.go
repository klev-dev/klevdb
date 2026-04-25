package klevdb

import (
	"context"
	"maps"
	"time"
)

// DeleteMultiBackoff is called on each iteration of [DeleteMulti] to give applications
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
// others a chance to work with the log, while being deleted
func DeleteMulti(ctx context.Context, l Log, offsets map[int64]struct{}, backoff DeleteMultiBackoff) ([]Message, int64, error) {
	var remainingOffsets = maps.Clone(offsets)
	var deletedMessages []Message
	var deletedSize int64

	for len(remainingOffsets) > 0 {
		deleted, size, err := l.Delete(remainingOffsets)
		switch {
		case err != nil:
			return deletedMessages, deletedSize, err
		case len(deleted) == 0:
			return deletedMessages, deletedSize, nil
		}

		deletedMessages = append(deletedMessages, deleted...)
		deletedSize += size
		for _, msg := range deleted {
			delete(remainingOffsets, msg.Offset)
		}

		if err := backoff(ctx); err != nil {
			return deletedMessages, deletedSize, err
		}
	}

	return deletedMessages, deletedSize, nil
}
