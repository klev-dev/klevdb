package klevdb

import (
	"context"
	"time"
)

func Compact(ctx context.Context, l Log, age time.Duration, boff DeleteMultiBackoff) error {
	updatesBefore := time.Now().Add(-age)
	if _, _, err := CompactUpdatesMultiOffsets(ctx, l, updatesBefore, boff); err != nil {
		return err
	}
	deletesBefore := time.Now().Add(-age * 2)
	if _, _, err := CompactDeletesMultiOffsets(ctx, l, deletesBefore, boff); err != nil {
		return err
	}
	return l.GC(0)
}
