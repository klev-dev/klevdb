package klevdb

import "context"

type TBlockingLog[K any, V any] interface {
	TLog[K, V]

	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)

	ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (nextOffset int64, messages []TMessage[K, V], err error)
}

func OpenTBlocking[K any, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (TBlockingLog[K, V], error) {
	l, err := OpenT(dir, opts, keyCodec, valueCodec)
	if err != nil {
		return nil, err
	}
	return WrapTBlocking(l)
}

func WrapTBlocking[K any, V any](l TLog[K, V]) (TBlockingLog[K, V], error) {
	next, err := l.NextOffset()
	if err != nil {
		return nil, err
	}
	return &tlogBlocking[K, V]{l, NewOffsetNotify(next)}, nil
}

type tlogBlocking[K any, V any] struct {
	TLog[K, V]
	notify *OffsetNotify
}

func (l *tlogBlocking[K, V]) Publish(tmessages []TMessage[K, V]) (int64, error) {
	nextOffset, err := l.TLog.Publish(tmessages)
	l.notify.Set(nextOffset)
	return nextOffset, err
}

func (l *tlogBlocking[K, V]) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.TLog.Consume(offset, maxCount)
}

func (l *tlogBlocking[K, V]) ConsumeByKeyBlocking(ctx context.Context, key K, empty bool, offset int64, maxCount int64) (int64, []TMessage[K, V], error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.TLog.ConsumeByKey(key, empty, offset, maxCount)
}
