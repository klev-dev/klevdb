package klevdb

import (
	"context"
	"fmt"
)

type BlockingLog interface {
	Log

	// ConsumeBlocking is similar to Consume, but if offset is equal to the next offsetit will block until next event is produced
	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)

	// ConsumeByKeyBlocking is similar to ConsumeBlocking, but only returns messages matching the key
	ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)
}

// TODO docs
func OpenBlocking(dir string, opts Options) (BlockingLog, error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.OpenBlocking] %s open: %w", dir, err)
	}
	w, err := WrapBlocking(l)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.OpenBlocking] %s wrap: %w", dir, err)
	}
	return w, nil
}

// TODO docs
func WrapBlocking(l Log) (BlockingLog, error) {
	next, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[klevdb.WrapBlocking] %s next offset: %w", l, err)
	}
	return &blockingLog{l, NewOffsetNotify(next)}, nil
}

type blockingLog struct {
	Log
	notify *OffsetNotify
}

func (l *blockingLog) Publish(messages []Message) (int64, error) {
	nextOffset, err := l.Log.Publish(messages)
	if err != nil {
		return OffsetInvalid, fmt.Errorf("[klevdb.BlockingLog.Publish] %s publish: %w", l.Log, err)
	}

	l.notify.Set(nextOffset)
	return nextOffset, nil
}

func (l *blockingLog) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.BlockingLog.ConsumeBlocking] %s wait: %w", l.Log, err)
	}
	next, msgs, err := l.Log.Consume(offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.BlockingLog.ConsumeBlocking] %s consume: %w", l.Log, err)
	}
	return next, msgs, nil
}

func (l *blockingLog) ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.BlockingLog.ConsumeByKeyBlocking] %s wait: %w", l.Log, err)
	}
	next, msgs, err := l.Log.ConsumeByKey(key, offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.BlockingLog.ConsumeByKeyBlocking] %s consume: %w", l.Log, err)
	}
	return next, msgs, nil
}

func (l *blockingLog) Close() error {
	if err := l.notify.Close(); err != nil {
		return fmt.Errorf("[klevdb.BlockingLog.Close] %s notify close: %w", l.Log, err)
	}
	if err := l.Log.Close(); err != nil {
		return fmt.Errorf("[klevdb.BlockingLog.Close] %s close: %w", l.Log, err)
	}
	return nil
}
