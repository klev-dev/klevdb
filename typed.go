package klevdb

import "time"

type TMessage[K any, V any] struct {
	Offset     int64
	Time       time.Time
	Key        K
	KeyEmpty   bool
	Value      V
	ValueEmpty bool
}

type TLog[K any, V any] interface {
	Publish(messages []TMessage[K, V]) (nextOffset int64, err error)

	NextOffset() (nextOffset int64, err error)

	Consume(offset int64, opts *ConsumeOptions) (nextOffset int64, messages []TMessage[K, V], err error)

	ConsumeByKey(key K, empty bool, offset int64, opts *ConsumeOptions) (nextOffset int64, messages []TMessage[K, V], err error)

	Get(offset int64) (message TMessage[K, V], err error)

	GetByKey(key K, empty bool) (message TMessage[K, V], err error)

	GetByTime(start time.Time) (message TMessage[K, V], err error)

	Delete(offsets map[int64]struct{}) (deletedOffsets map[int64]struct{}, deletedSize int64, err error)

	Size(m Message) int64

	Stat() (Stats, error)

	Backup(dir string) error

	Sync() (nextOffset int64, err error)

	GC(unusedFor time.Duration) error

	Close() error
}

func OpenT[K any, V any](dir string, opts Options, keyCodec Codec[K], valueCodec Codec[V]) (TLog[K, V], error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &tlog[K, V]{l, keyCodec, valueCodec}, nil
}

type tlog[K any, V any] struct {
	Log

	keyCodec   Codec[K]
	valueCodec Codec[V]
}

func (l *tlog[K, V]) Publish(tmessages []TMessage[K, V]) (int64, error) {
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

func (l *tlog[K, V]) Consume(offset int64, opts *ConsumeOptions) (int64, []TMessage[K, V], error) {
	nextOffset, messages, err := l.Log.Consume(offset, opts)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	if len(messages) == 0 {
		return nextOffset, nil, nil
	}

	tmessages := make([]TMessage[K, V], len(messages))
	for i, msg := range messages {
		tmessages[i], err = l.decode(msg)
		if err != nil {
			return OffsetInvalid, nil, err
		}
	}
	return nextOffset, tmessages, nil
}

func (l *tlog[K, V]) ConsumeByKey(key K, empty bool, offset int64, opts *ConsumeOptions) (int64, []TMessage[K, V], error) {
	kbytes, err := l.keyCodec.Encode(key, empty)
	if err != nil {
		return OffsetInvalid, nil, err
	}

	nextOffset, messages, err := l.Log.ConsumeByKey(kbytes, offset, opts)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	if len(messages) == 0 {
		return nextOffset, nil, nil
	}

	tmessages := make([]TMessage[K, V], len(messages))
	for i, msg := range messages {
		tmessages[i], err = l.decode(msg)
		if err != nil {
			return OffsetInvalid, nil, err
		}
	}
	return nextOffset, tmessages, nil
}

func (l *tlog[K, V]) Get(offset int64) (TMessage[K, V], error) {
	msg, err := l.Log.Get(offset)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *tlog[K, V]) GetByKey(key K, empty bool) (TMessage[K, V], error) {
	kbytes, err := l.keyCodec.Encode(key, empty)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}
	msg, err := l.Log.GetByKey(kbytes)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *tlog[K, V]) GetByTime(start time.Time) (TMessage[K, V], error) {
	msg, err := l.Log.GetByTime(start)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *tlog[K, V]) encode(tmsg TMessage[K, V]) (msg Message, err error) {
	msg.Offset = tmsg.Offset
	msg.Time = tmsg.Time

	msg.Key, err = l.keyCodec.Encode(tmsg.Key, tmsg.KeyEmpty)
	if err != nil {
		return InvalidMessage, nil
	}

	msg.Value, err = l.valueCodec.Encode(tmsg.Value, tmsg.ValueEmpty)
	if err != nil {
		return InvalidMessage, nil
	}

	return msg, nil
}

func (l *tlog[K, V]) decode(msg Message) (tmsg TMessage[K, V], err error) {
	tmsg.Offset = msg.Offset
	tmsg.Time = msg.Time

	tmsg.Key, tmsg.KeyEmpty, err = l.keyCodec.Decode(msg.Key)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}

	tmsg.Value, tmsg.ValueEmpty, err = l.valueCodec.Decode(msg.Value)
	if err != nil {
		return TMessage[K, V]{Offset: OffsetInvalid}, err
	}

	return tmsg, nil
}
