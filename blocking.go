package klevdb

import "context"

// BlockingLog enhances [Log] adding blocking consume
type BlockingLog interface {
	Log

	// ConsumeBlocking is similar to [Consume], but if offset is equal to the next offset it will block until next message is produced
	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)

	// ConsumeByKeyBlocking is similar to [ConsumeBlocking], but only returns messages matching the key
	ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)
}

// OpenBlocking opens a [Log] and wraps it with support for blocking consume
func OpenBlocking(dir string, opts Options) (BlockingLog, error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return WrapBlocking(l)
}

// WrapBlocking wraps a [Log] with support for blocking consume
func WrapBlocking(l Log) (BlockingLog, error) {
	next, err := l.NextOffset()
	if err != nil {
		return nil, err
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
		return OffsetInvalid, err
	}

	l.notify.Set(nextOffset)
	return nextOffset, nil
}

func (l *blockingLog) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.Consume(offset, maxCount)
}

func (l *blockingLog) ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.ConsumeByKey(key, offset, maxCount)
}

func (l *blockingLog) Close() error {
	if err := l.notify.Close(); err != nil {
		return err
	}
	return l.Log.Close()
}
