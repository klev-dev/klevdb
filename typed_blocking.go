package klevdb

import (
	"context"
	"fmt"
)

type TBlockingLog[K any, V any] interface {
	TLog[K, V]

	// ConsumeBlocking is similar to Consume, but if offset is equal to the next offsetit will block until next event is produced
	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)

	// ConsumeByKeyBlocking is similar to ConsumeBlocking, but only returns messages matching the key
	ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)
}

// TODO docs
func OpenTBlocking[K any, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (TBlockingLog[K, V], error) {
	l, err := OpenT(dir, opts, keyCodec, valueCodec)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.OpenTBlocking] %s open: %w", dir, err)
	}
	w, err := WrapTBlocking(l)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.OpenTBlocking] %s wrap: %w", dir, err)
	}
	return w, nil
}

// TODO docs
func WrapTBlocking[K any, V any](l TLog[K, V]) (TBlockingLog[K, V], error) {
	next, err := l.NextOffset()
	if err != nil {
		return nil, fmt.Errorf("[klevdb.WrapTBlocking] %s next offset: %w", l, err)
	}
	return &tlogBlocking[K, V]{l, NewOffsetNotify(next)}, nil
}

type tlogBlocking[K any, V any] struct {
	TLog[K, V]
	notify *OffsetNotify
}

func (l *tlogBlocking[K, V]) Publish(tmessages []TMessage[K, V]) (int64, error) {
	nextOffset, err := l.TLog.Publish(tmessages)
	if err != nil {
		return OffsetInvalid, fmt.Errorf("[klevdb.TBlockingLog.Publish] %s publish: %w", l.TLog, err)
	}

	l.notify.Set(nextOffset)
	return nextOffset, nil
}

func (l *tlogBlocking[K, V]) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.TBlockingLog.ConsumeBlocking] %s wait: %w", l.TLog, err)
	}
	next, msgs, err := l.TLog.Consume(offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.TBlockingLog.ConsumeBlocking] %s consume: %w", l.TLog, err)
	}
	return next, msgs, nil
}

func (l *tlogBlocking[K, V]) ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.TBlockingLog.ConsumeByKeyBlocking] %s wait: %w", l.TLog, err)
	}
	next, msgs, err := l.TLog.ConsumeByKey(key, empty, offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, fmt.Errorf("[klevdb.TBlockingLog.ConsumeByKeyBlocking] %s consume: %w", l.TLog, err)
	}
	return next, msgs, nil
}

func (l *tlogBlocking[K, V]) Close() error {
	if err := l.notify.Close(); err != nil {
		return fmt.Errorf("[klevdb.TBlockingLog.Close] %s notify close: %w", l.TLog, err)
	}
	if err := l.TLog.Close(); err != nil {
		return fmt.Errorf("[klevdb.TBlockingLog.Close] %s close: %w", l.TLog, err)
	}
	return nil
}
