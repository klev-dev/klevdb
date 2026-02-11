package klevdb

import (
	"context"

	"github.com/klev-dev/klevdb/notify"
)

// TBlockingLog enhances [TLog] adding blocking consume
type TBlockingLog[K any, V any] interface {
	TLog[K, V]

	// ConsumeBlocking see [BlockingLog.ConsumeBlocking]
	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)

	// ConsumeByKeyBlocking see [BlockingLog.ConsumeByKeyBlocking]
	ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)
}

// OpenTBlocking opens tlog and wraps it with support for blocking consume
func OpenTBlocking[K any, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (TBlockingLog[K, V], error) {
	l, err := OpenT(dir, opts, keyCodec, valueCodec)
	if err != nil {
		return nil, err
	}
	return WrapTBlocking(l)
}

// WrapTBlocking wraps tlog with support for blocking consume
func WrapTBlocking[K any, V any](l TLog[K, V]) (TBlockingLog[K, V], error) {
	next, err := l.NextOffset()
	if err != nil {
		return nil, err
	}
	return &tlogBlocking[K, V]{l, notify.NewOffset(next)}, nil
}

type tlogBlocking[K any, V any] struct {
	TLog[K, V]
	notify *notify.Offset
}

func (l *tlogBlocking[K, V]) Publish(tmessages []TMessage[K, V]) (int64, error) {
	nextOffset, err := l.TLog.Publish(tmessages)
	if err != nil {
		return OffsetInvalid, err
	}

	l.notify.Set(nextOffset)
	return nextOffset, err
}

func (l *tlogBlocking[K, V]) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.Consume(offset, maxCount)
}

func (l *tlogBlocking[K, V]) ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.ConsumeByKey(key, empty, offset, maxCount)
}

func (l *tlogBlocking[K, V]) Close() error {
	if err := l.notify.Close(); err != nil {
		return err
	}
	return l.TLog.Close()
}
