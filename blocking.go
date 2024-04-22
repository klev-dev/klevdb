package klevdb

import "context"

type BlockingLog interface {
	Log

	// ConsumeBlocking is similar to Consume, but if offset is equal to the next offsetit will block until next event is produced
	ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)

	// ConsumeByKeyBlocking is similar to ConsumeBlocking, but only returns messages matching the key
	ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)
}

func OpenBlocking(dir string, opts Options) (BlockingLog, error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return WrapBlocking(l)
}

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
	l.notify.Set(nextOffset)
	return nextOffset, err
}

func (l *blockingLog) ConsumeBlocking(ctx context.Context, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.Log.Consume(offset, maxCount)
}

func (l *blockingLog) ConsumeByKeyBlocking(ctx context.Context, key []byte, offset int64, maxCount int64) (int64, []Message, error) {
	if err := l.notify.Wait(ctx, offset); err != nil {
		return 0, nil, err
	}
	return l.Log.ConsumeByKey(key, offset, maxCount)
}
