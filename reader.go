package klevdb

import (
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
)

type reader struct {
	segment segment.Segment
	params  index.Params
	head    bool

	messages      *message.Reader
	messagesMu    sync.RWMutex
	messagesInuse int64

	index           indexer
	indexMu         sync.RWMutex
	indexLastAccess int64
}

type indexer interface {
	GetNextOffset() (int64, error)
	Consume(offset int64) (int64, int64, int64, error)
	Get(offset int64) (int64, error)
	Keys(hash []byte) ([]int64, error)
	Time(ts int64) (int64, error)
	Len() int
}

func openReader(seg segment.Segment, params index.Params, head bool) *reader {
	return &reader{
		segment: seg,
		params:  params,
		head:    head,
	}
}

func reopenReader(seg segment.Segment, params index.Params, ix indexer) *reader {
	return &reader{
		segment: seg,
		params:  params,
		head:    false,

		index: ix,
	}
}

func openReaderAppend(seg segment.Segment, params index.Params, ix indexer) (*reader, error) {
	messages, err := message.OpenReader(seg.Log)
	if err != nil {
		return nil, err
	}

	return &reader{
		segment: seg,
		params:  params,
		head:    true,

		messages: messages,
		index:    ix,
	}, nil
}

func (r *reader) GetOffset() int64 {
	return r.segment.GetOffset()
}

func (r *reader) GetNextOffset() (int64, error) {
	index, err := r.getIndexNow()
	if err != nil {
		return 0, err
	}
	return index.GetNextOffset()
}

func (r *reader) Consume(offset, maxCount int64) (int64, []message.Message, error) {
	index, err := r.getIndexNow()
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
	defer atomic.AddInt64(&r.messagesInuse, -1)

	msgs, err := messages.Consume(position, maxPosition, maxCount)
	if err != nil {
		return OffsetInvalid, nil, err
	}
	return msgs[len(msgs)-1].Offset + 1, msgs, nil
}

func (r *reader) ConsumeByKey(key, keyHash []byte, offset, maxCount int64) (int64, []message.Message, error) {
	ix, err := r.getIndexNow()
	if err != nil {
		return OffsetInvalid, nil, err
	}

	if offset == OffsetNewest {
		nextOffset, err := ix.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	}

	positions, err := ix.Keys(keyHash)
	switch {
	case err == nil:
		break
	case err == index.ErrKeyNotFound:
		nextOffset, err := ix.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	default:
		return OffsetInvalid, nil, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return OffsetInvalid, nil, err
	}
	defer atomic.AddInt64(&r.messagesInuse, -1)

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
		nextOffset, err := ix.GetNextOffset()
		if err != nil {
			return OffsetInvalid, nil, err
		}
		return nextOffset, nil, nil
	}

	return msgs[len(msgs)-1].Offset + 1, msgs, nil
}

func (r *reader) Get(offset int64) (message.Message, error) {
	index, err := r.getIndexNow()
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
	defer atomic.AddInt64(&r.messagesInuse, -1)

	return messages.Get(position)
}

func (r *reader) GetByKey(key, keyHash []byte, tctx int64) (message.Message, error) {
	ix, err := r.getIndexAt(tctx)
	if err != nil {
		return message.Invalid, err
	}

	positions, err := ix.Keys(keyHash)
	if err != nil {
		return message.Invalid, err
	}

	messages, err := r.getMessages()
	if err != nil {
		return message.Invalid, err
	}
	defer atomic.AddInt64(&r.messagesInuse, -1)

	for i := len(positions) - 1; i >= 0; i-- {
		msg, err := messages.Get(positions[i])
		if err != nil {
			return message.Invalid, err
		}
		if bytes.Equal(key, msg.Key) {
			return msg, nil
		}
	}

	return message.Invalid, index.ErrKeyNotFound
}

func (r *reader) GetByTime(ts int64, tctx int64) (message.Message, error) {
	index, err := r.getIndexAt(tctx)
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
	defer atomic.AddInt64(&r.messagesInuse, -1)

	return messages.Get(position)
}

func (r *reader) Stat() (segment.Stats, error) {
	return r.segment.Stat(r.params)
}

func (r *reader) Backup(dir string) error {
	return r.segment.Backup(dir)
}

func (r *reader) Delete(rs *segment.RewriteSegment) (*reader, error) {
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

		return &reader{segment: nseg, params: r.params}, nil
	}

	// the rewritten segment has the same starting offset
	if err := rs.Segment.Override(r.segment); err != nil {
		return nil, err
	}

	return r, nil
}

func (r *reader) getIndexAt(tctx int64) (indexer, error) {
	atomic.StoreInt64(&r.indexLastAccess, tctx)
	return r.getIndexMarked()
}

func (r *reader) getIndexNow() (indexer, error) {
	atomic.StoreInt64(&r.indexLastAccess, time.Now().UnixMicro())
	return r.getIndexMarked()
}

func (r *reader) getIndexMarked() (indexer, error) {
	r.indexMu.RLock()
	if ix := r.index; ix != nil {
		defer r.indexMu.RUnlock()
		return ix, nil
	}
	r.indexMu.RUnlock()

	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	if ix := r.index; ix != nil {
		return ix, nil
	}

	items, err := r.segment.ReindexAndReadIndex(r.params)
	if err != nil {
		return nil, err
	}

	r.index = newReaderIndex(items, r.params.Keys, r.segment.Offset, r.head)
	return r.index, nil
}

func (r *reader) getMessages() (*message.Reader, error) {
	r.messagesMu.RLock()
	if msgs := r.messages; msgs != nil {
		atomic.AddInt64(&r.messagesInuse, 1)
		r.messagesMu.RUnlock()
		return msgs, nil
	}
	r.messagesMu.RUnlock()

	r.messagesMu.Lock()
	defer r.messagesMu.Unlock()

	if msgs := r.messages; msgs != nil {
		atomic.AddInt64(&r.messagesInuse, 1)
		return msgs, nil
	}

	msgs, err := message.OpenReaderMem(r.segment.Log)
	if err != nil {
		return nil, err
	}

	r.messages = msgs
	atomic.AddInt64(&r.messagesInuse, 1)
	return msgs, nil
}

func (r *reader) closeIndex() {
	r.indexMu.Lock()
	defer r.indexMu.Unlock()

	r.index = nil
}

func (r *reader) GC(unusedFor time.Duration) error {
	if r.head {
		// we never GC an actively writing segment
		return nil
	}

	indexLastAccess := time.UnixMicro(atomic.LoadInt64(&r.indexLastAccess))
	if time.Since(indexLastAccess) < unusedFor {
		// only unload segments unused for defined time
		return nil
	}

	r.closeIndex()

	r.messagesMu.Lock()
	defer r.messagesMu.Unlock()

	if r.messages == nil || atomic.LoadInt64(&r.messagesInuse) > 0 {
		return nil
	}

	if err := r.messages.Close(); err != nil {
		return err
	}
	r.messages = nil
	return nil
}

func (r *reader) Close() error {
	r.closeIndex()

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

type readerIndex struct {
	items      []index.Item
	keys       art.Tree
	nextOffset int64
	head       bool
}

func newReaderIndex(items []index.Item, hasKeys bool, offset int64, head bool) *readerIndex {
	var keys art.Tree
	if hasKeys {
		keys = art.New()
		index.AppendKeys(keys, items)
	}

	nextOffset := offset
	if len(items) > 0 {
		nextOffset = items[len(items)-1].Offset + 1
	}

	return &readerIndex{
		items:      items,
		keys:       keys,
		nextOffset: nextOffset,
		head:       head,
	}
}

func (ix *readerIndex) GetNextOffset() (int64, error) {
	return ix.nextOffset, nil
}

func (ix *readerIndex) Consume(offset int64) (int64, int64, int64, error) {
	position, maxPosition, err := index.Consume(ix.items, offset)
	if (err == index.ErrOffsetIndexEmpty || err == index.ErrOffsetAfterEnd) && ix.head && offset <= ix.nextOffset {
		return -1, -1, ix.nextOffset, nil
	}
	return position, maxPosition, offset, err
}

func (ix *readerIndex) Get(offset int64) (int64, error) {
	position, err := index.Get(ix.items, offset)
	if err == index.ErrOffsetAfterEnd && ix.head && offset >= ix.nextOffset {
		return -1, message.ErrInvalidOffset
	}
	return position, err
}

func (ix *readerIndex) Keys(keyHash []byte) ([]int64, error) {
	return index.Keys(ix.keys, keyHash)
}

func (ix *readerIndex) Time(ts int64) (int64, error) {
	return index.Time(ix.items, ts)
}

func (ix *readerIndex) Len() int {
	return len(ix.items)
}
