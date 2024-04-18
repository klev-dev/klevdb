package klevdb

import "time"

type VMessage[V any] struct {
	Offset int64
	Time   time.Time
	Value  V
}

type VLog[V any] interface {
	Publish(messages []VMessage[V]) (nextOffset int64, err error)

	NextOffset() (nextOffset int64, err error)

	Consume(offset int64, maxCount int64) (nextOffset int64, messages []VMessage[V], err error)

	Get(offset int64) (message VMessage[V], err error)

	GetByTime(start time.Time) (message VMessage[V], err error)

	Delete(offsets map[int64]struct{}) (deletedOffsets map[int64]struct{}, deletedSize int64, err error)

	Size(m Message) int64

	Stat() (Stats, error)

	Backup(dir string) error

	Sync() (nextOffset int64, err error)

	GC(unusedFor time.Duration) error

	Close() error
}

func OpenV[V any](dir string, opts Options, valueCodec Codec[V]) (VLog[V], error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &vlog[V]{l, valueCodec}, nil
}

type vlog[V any] struct {
	Log

	valueCodec Codec[V]
}

func (l *vlog[V]) Publish(tmessages []VMessage[V]) (int64, error) {
	var err error
	messages := make([]Message, len(tmessages))
	for i, tmsg := range tmessages {
		messages[i], err = l.encode(tmsg)
		if err != nil {
			return OffsetInvalid, err
		}
	}

	return l.Log.Publish(messages)
}

func (l *vlog[V]) Consume(offset int64, maxCount int64) (int64, []VMessage[V], error) {
	nextOffset, messages, err := l.Log.Consume(offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	if len(messages) == 0 {
		return nextOffset, nil, nil
	}

	tmessages := make([]VMessage[V], len(messages))
	for i, msg := range messages {
		tmessages[i], err = l.decode(msg)
		if err != nil {
			return OffsetInvalid, nil, err
		}
	}
	return nextOffset, tmessages, nil
}

func (l *vlog[V]) Get(offset int64) (VMessage[V], error) {
	msg, err := l.Log.Get(offset)
	if err != nil {
		return VMessage[V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *vlog[V]) GetByTime(start time.Time) (VMessage[V], error) {
	msg, err := l.Log.GetByTime(start)
	if err != nil {
		return VMessage[V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *vlog[V]) encode(tmsg VMessage[V]) (msg Message, err error) {
	msg.Offset = tmsg.Offset
	msg.Time = tmsg.Time

	msg.Value, err = l.valueCodec.Encode(tmsg.Value)
	if err != nil {
		return InvalidMessage, nil
	}

	return msg, nil
}

func (l *vlog[V]) decode(msg Message) (tmsg VMessage[V], err error) {
	tmsg.Offset = msg.Offset
	tmsg.Time = msg.Time

	tmsg.Value, err = l.valueCodec.Decode(msg.Value)
	if err != nil {
		return VMessage[V]{Offset: OffsetInvalid}, err
	}

	return tmsg, nil
}
