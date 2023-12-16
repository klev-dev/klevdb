package klevdb

import (
	"bytes"
	"errors"
	"sync"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
)

type reader[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	segment segment.Segment[IX, IT, IC, IS]
	ix      IX
	keys    bool
	head    bool

	messages   *message.Reader
	messagesMu sync.RWMutex

	index   indexer
	indexMu sync.RWMutex
}

type indexer interface {
	GetNextOffset() (int64, error)
	Consume(offset int64) (int64, int64, int64, error)
	Get(offset int64) (int64, error)
	Keys(hash []byte) ([]int64, error)
	Time(ts int64) (int64, error)
	Len() int
}

func openReader[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](seg segment.Segment[IX, IT, IC, IS], ix IX, keys bool, head bool) *reader[IX, IT, IC, IS] {
	return &reader[IX, IT, IC, IS]{
		segment: seg,
		ix:      ix,
		keys:    keys,
		head:    head,
	}
}

func reopenReader[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](seg segment.Segment[IX, IT, IC, IS], ix IX, keys bool, ixr indexer) *reader[IX, IT, IC, IS] {
	return &reader[IX, IT, IC, IS]{
		segment: seg,
		ix:      ix,
		keys:    keys,
		head:    false,

		index: ixr,
	}
}

func openReaderAppend[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](seg segment.Segment[IX, IT, IC, IS], ix IX, keys bool, ixr indexer) (*reader[IX, IT, IC, IS], error) {
	messages, err := message.OpenReader(seg.Log)
	if err != nil {
		return nil, err
	}

	return &reader[IX, IT, IC, IS]{
		segment: seg,
		ix:      ix,
		keys:    keys,
		head:    true,

		messages: messages,
		index:    ixr,
	}, nil
}

func (r *reader[IX, IT, IC, IS]) GetOffset() int64 {
	return r.segment.GetOffset()
}

func (r *reader[IX, IT, IC, IS]) GetNextOffset() (int64, error) {
	index, err := r.getIndex()
	if err != nil {
		return 0, err
	}
	return index.GetNextOffset()
}

func (r *reader[IX, IT, IC, IS]) Consume(offset, maxCount int64) (int64, []message.Message, error) {
	index, err := r.getIndex()
	if err != nil {
		return OffsetInvalid, nil, err
	}

	if offset == OffsetNewest {
		nextOffset, err := index.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	}

	position, maxPosition, nextOffset, err := index.Consume(offset)
	switch {
	case err != nil:
		return OffsetInvalid, nil, err
	case position == -1:
		return nextOffset, nil, nil
	}

	messages, err := r.getMessages()
	if err != nil {
		return OffsetInvalid, nil, err
	}

	msgs, err := messages.Consume(position, maxPosition, maxCount)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	return msgs[len(msgs)-1].Offset + 1, msgs, nil
}

func (r *reader[IX, IT, IC, IS]) ConsumeByKey(key, keyHash []byte, offset, maxCount int64) (int64, []message.Message, error) {
	index, err := r.getIndex()
	if err != nil {
		return OffsetInvalid, nil, err
	}

	if offset == OffsetNewest {
		nextOffset, err := index.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	}

	positions, err := index.Keys(keyHash)
	if err != nil {
		if errors.Is(err, message.ErrNotFound) {
			nextOffset, err := index.GetNextOffset()
			if err != nil {
				return OffsetInvalid, nil, err
			}
			return nextOffset, nil, nil
		}
		return OffsetInvalid, nil, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return OffsetInvalid, nil, err
	}

	var msgs []message.Message
	for i := 0; i < len(positions); i++ {
		msg, err := messages.Get(positions[i])
		if err != nil {
			return OffsetInvalid, nil, err
		}
		if msg.Offset < offset {
			continue
		}
		if bytes.Equal(key, msg.Key) {
			msgs = append(msgs, msg)
			if len(msgs) >= int(maxCount) {
				break
			}
		}
	}

	if len(msgs) == 0 {
		nextOffset, err := index.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	}

	return msgs[len(msgs)-1].Offset + 1, msgs, nil
}

func (r *reader[IX, IT, IC, IS]) Get(offset int64) (message.Message, error) {
	index, err := r.getIndex()
	if err != nil {
		return message.Invalid, err
	}

	position, err := index.Get(offset)
	if err != nil {
		return message.Invalid, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return message.Invalid, err
	}

	return messages.Get(position)
}

func (r *reader[IX, IT, IC, IS]) GetByKey(key, keyHash []byte) (message.Message, error) {
	index, err := r.getIndex()
	if err != nil {
		return message.Invalid, err
	}

	positions, err := index.Keys(keyHash)
	if err != nil {
		return message.Invalid, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return message.Invalid, err
	}

	for i := len(positions) - 1; i >= 0; i-- {
		msg, err := messages.Get(positions[i])
		if err != nil {
			return message.Invalid, err
		}
		if bytes.Equal(key, msg.Key) {
			return msg, nil
		}
	}

	return message.Invalid, message.ErrNotFound
}

func (r *reader[IX, IT, IC, IS]) GetByTime(ts int64) (message.Message, error) {
	index, err := r.getIndex()
	if err != nil {
		return message.Invalid, err
	}

	position, err := index.Time(ts)
	if err != nil {
		return message.Invalid, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return message.Invalid, err
	}

	return messages.Get(position)
}

func (r *reader[IX, IT, IC, IS]) Stat() (segment.Stats, error) {
	return r.segment.Stat(r.ix)
}

func (r *reader[IX, IT, IC, IS]) Backup(dir string) error {
	return r.segment.Backup(dir)
}

func (r *reader[IX, IT, IC, IS]) Delete(rs *segment.RewriteSegment[IX, IT, IC, IS]) (*reader[IX, IT, IC, IS], error) {
	// log already has reader lock exclusively, no need to sync here
	if err := r.Close(); err != nil {
		return nil, err
	}

	if len(rs.SurviveOffsets) == 0 {
		// nothing left in reader, drop empty files
		if err := rs.Segment.Remove(); err != nil {
			return nil, err
		}

		return nil, r.segment.Remove()
	}

	nseg := rs.GetNewSegment()
	if nseg != r.segment {
		// the starting offset of the new segment is different

		// first move the replacement
		if err := rs.Segment.Rename(nseg); err != nil {
			return nil, err
		}

		// then delete this segment
		if err := r.segment.Remove(); err != nil {
			return nil, err
		}

		return &reader[IX, IT, IC, IS]{segment: nseg, ix: r.ix, keys: r.keys}, nil
	}

	// the rewritten segment has the same starting offset
	if err := rs.Segment.Override(r.segment); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *reader[IX, IT, IC, IS]) getIndex() (indexer, error) {
	r.indexMu.RLock()
	if ix := r.index; ix != nil {
		r.indexMu.RUnlock()
		return ix, nil
	}
	r.indexMu.RUnlock()

	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	if ix := r.index; ix != nil {
		return ix, nil
	}

	items, err := r.segment.ReindexAndReadIndex(r.ix)
	if err != nil {
		return nil, err
	}

	r.index = newReaderIndex[IX, IT, IC](items, r.keys, r.segment.Offset, r.head)
	return r.index, nil
}

func (r *reader[IX, IT, IC, IS]) getMessages() (*message.Reader, error) {
	r.messagesMu.RLock()
	if msgs := r.messages; msgs != nil {
		r.messagesMu.RUnlock()
		return msgs, nil
	}
	r.messagesMu.RUnlock()

	r.messagesMu.Lock()
	defer r.messagesMu.Unlock()

	if msgs := r.messages; msgs != nil {
		return msgs, nil
	}

	msgs, err := message.OpenReaderMem(r.segment.Log)
	if err != nil {
		return nil, err
	}

	r.messages = msgs
	return r.messages, nil
}

func (r *reader[IX, IT, IC, IS]) Close() error {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	r.index = nil

	r.messagesMu.Lock()
	defer r.messagesMu.Unlock()

	if r.messages == nil {
		return nil
	}

	if err := r.messages.Close(); err != nil {
		return err
	}
	r.messages = nil
	return nil
}

type readerIndex[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	items      []IT
	keys       art.Tree
	nextOffset int64
	head       bool
}

func newReaderIndex[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](items []IT, hasKeys bool, offset int64, head bool) *readerIndex[IX, IT, IC, IS] {
	var keys art.Tree
	if hasKeys {
		keys = art.New()
		index.AppendKeys(keys, items)
	}

	nextOffset := offset
	if len(items) > 0 {
		nextOffset = items[len(items)-1].Offset() + 1
	}

	return &readerIndex[IX, IT, IC, IS]{
		items:      items,
		keys:       keys,
		nextOffset: nextOffset,
		head:       head,
	}
}

func (rix *readerIndex[IX, IT, IC, IS]) GetNextOffset() (int64, error) {
	return rix.nextOffset, nil
}

func (rix *readerIndex[IX, IT, IC, IS]) Consume(offset int64) (int64, int64, int64, error) {
	position, maxPosition, err := index.Consume(rix.items, offset)
	if err != nil && rix.head {
		switch {
		case err == index.ErrIndexEmpty:
			if offset <= rix.nextOffset {
				return -1, -1, rix.nextOffset, nil
			}
		case err == message.ErrInvalidOffset:
			if offset == rix.nextOffset {
				return -1, -1, rix.nextOffset, nil
			}
		}
	}
	return position, maxPosition, offset, err
}

func (rix *readerIndex[IX, IT, IC, IS]) Get(offset int64) (int64, error) {
	position, err := index.Get(rix.items, offset)
	if err == message.ErrNotFound && rix.head && offset >= rix.nextOffset {
		return -1, message.ErrInvalidOffset
	}
	return position, err
}

func (rix *readerIndex[IX, IT, IC, IS]) Keys(keyHash []byte) ([]int64, error) {
	return index.Keys(rix.keys, keyHash)
}

func (rix *readerIndex[IX, IT, IC, IS]) Time(ts int64) (int64, error) {
	return index.Time(rix.items, ts)
}

func (rix *readerIndex[IX, IT, IC, IS]) Len() int {
	return len(rix.items)
}
