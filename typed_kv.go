package klevdb

import "time"

type KVMessage[K any, V any] struct {
	Offset   int64
	Time     time.Time
	Key      K
	HasValue bool
	Value    V
}

type KVLog[K any, V any] interface {
	Publish(messages []KVMessage[K, V]) (nextOffset int64, err error)

	NextOffset() (nextOffset int64, err error)

	Consume(offset int64, maxCount int64) (nextOffset int64, messages []KVMessage[K, V], err error)

	ConsumeByKey(key K, offset int64, maxCount int64) (nextOffset int64, messages []KVMessage[K, V], err error)

	Get(offset int64) (message KVMessage[K, V], err error)

	GetByKey(key K) (message KVMessage[K, V], err error)

	GetByTime(start time.Time) (message KVMessage[K, V], err error)

	Delete(offsets map[int64]struct{}) (deletedOffsets map[int64]struct{}, deletedSize int64, err error)

	Size(m Message) int64

	Stat() (Stats, error)

	Backup(dir string) error

	Sync() (nextOffset int64, err error)

	GC(unusedFor time.Duration) error

	Close() error
}

func OpenKV[K any, V any](dir string, opts Options, keyCodec Codec[K], valueCodec OptCodec[V]) (KVLog[K, V], error) {
	l, err := Open(dir, opts)
	if err != nil {
		return nil, err
	}
	return &kvlog[K, V]{l, keyCodec, valueCodec}, nil
}

type kvlog[K any, V any] struct {
	Log

	keyCodec   Codec[K]
	valueCodec OptCodec[V]
}

func (l *kvlog[K, V]) Publish(tmessages []KVMessage[K, V]) (int64, error) {
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

func (l *kvlog[K, V]) Consume(offset int64, maxCount int64) (int64, []KVMessage[K, V], error) {
	nextOffset, messages, err := l.Log.Consume(offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	if len(messages) == 0 {
		return nextOffset, nil, nil
	}

	tmessages := make([]KVMessage[K, V], len(messages))
	for i, msg := range messages {
		tmessages[i], err = l.decode(msg)
		if err != nil {
			return OffsetInvalid, nil, err
		}
	}
	return nextOffset, tmessages, nil
}

func (l *kvlog[K, V]) ConsumeByKey(key K, offset int64, maxCount int64) (int64, []KVMessage[K, V], error) {
	kbytes, err := l.keyCodec.Encode(key)
	if err != nil {
		return OffsetInvalid, nil, err
	}

	nextOffset, messages, err := l.Log.ConsumeByKey(kbytes, offset, maxCount)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	if len(messages) == 0 {
		return nextOffset, nil, nil
	}

	tmessages := make([]KVMessage[K, V], len(messages))
	for i, msg := range messages {
		tmessages[i], err = l.decode(msg)
		if err != nil {
			return OffsetInvalid, nil, err
		}
	}
	return nextOffset, tmessages, nil
}

func (l *kvlog[K, V]) Get(offset int64) (KVMessage[K, V], error) {
	msg, err := l.Log.Get(offset)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *kvlog[K, V]) GetByKey(key K) (KVMessage[K, V], error) {
	kbytes, err := l.keyCodec.Encode(key)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}
	msg, err := l.Log.GetByKey(kbytes)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *kvlog[K, V]) GetByTime(start time.Time) (KVMessage[K, V], error) {
	msg, err := l.Log.GetByTime(start)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}
	return l.decode(msg)
}

func (l *kvlog[K, V]) encode(tmsg KVMessage[K, V]) (msg Message, err error) {
	msg.Offset = tmsg.Offset
	msg.Time = tmsg.Time

	msg.Key, err = l.keyCodec.Encode(tmsg.Key)
	if err != nil {
		return InvalidMessage, nil
	}

	msg.Value, err = l.valueCodec.EncodeOpt(tmsg.Value, tmsg.HasValue)
	if err != nil {
		return InvalidMessage, nil
	}

	return msg, nil
}

func (l *kvlog[K, V]) decode(msg Message) (tmsg KVMessage[K, V], err error) {
	tmsg.Offset = msg.Offset
	tmsg.Time = msg.Time

	tmsg.Key, err = l.keyCodec.Decode(msg.Key)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}

	tmsg.Value, tmsg.HasValue, err = l.valueCodec.DecodeOpt(msg.Value)
	if err != nil {
		return KVMessage[K, V]{Offset: OffsetInvalid}, err
	}

	return tmsg, nil
}
