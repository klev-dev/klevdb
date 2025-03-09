package klevdb

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
)

type writer struct {
	segment segment.Segment
	params  index.Params

	messages *message.Writer
	items    *index.Writer
	index    *writerIndex
	reader   *reader
}

func openWriter(seg segment.Segment, params index.Params, nextTime int64) (*writer, error) {
	messages, err := message.OpenWriter(seg.Log)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.openWriter] %s messages open: %w", seg.Log, err)
	}

	var ix *writerIndex
	if messages.Size() > 0 {
		indexItems, err := seg.ReindexAndReadIndex(params)
		if err != nil {
			return nil, fmt.Errorf("[klevdb.openWriter] %s reindex: %w", seg.Index, err)
		}
		ix = newWriterIndex(indexItems, params.Keys, seg.Offset, nextTime)
	} else {
		ix = newWriterIndex(nil, params.Keys, seg.Offset, nextTime)
	}

	items, err := index.OpenWriter(seg.Index, params)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.openWriter] %s index open: %w", seg.Index, err)
	}

	reader, err := openReaderAppend(seg, params, ix)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.openWriter] %d reader open: %w", seg.Offset, err)
	}

	return &writer{
		segment: seg,
		params:  params,

		messages: messages,
		items:    items,
		index:    ix,
		reader:   reader,
	}, nil
}

func (w *writer) GetNextOffset() (int64, error) {
	nextOffset, err := w.index.GetNextOffset()
	if err != nil {
		return OffsetInvalid, fmt.Errorf("[klevdb.writer.GetNextOffset] get: %w", err)
	}
	return nextOffset, nil
}

func (w *writer) NeedsRollover(rollover int64) bool {
	return (w.messages.Size() + w.items.Size()) > rollover
}

func (w *writer) Publish(msgs []message.Message) (int64, error) {
	nextOffset, indexTime := w.index.getNext()

	items := make([]index.Item, len(msgs))
	for i := range msgs {
		msgs[i].Offset = nextOffset + int64(i)
		if msgs[i].Time.IsZero() {
			msgs[i].Time = time.Now().UTC()
		}

		position, err := w.messages.Write(msgs[i])
		if err != nil {
			return OffsetInvalid, fmt.Errorf("[klevdb.writer.Publish] messages write: %w", err)
		}

		items[i] = w.params.NewItem(msgs[i], position, indexTime)
		if err := w.items.Write(items[i]); err != nil {
			return OffsetInvalid, fmt.Errorf("[klevdb.writer.Publish] index write: %w", err)
		}
		indexTime = items[i].Timestamp
	}

	return w.index.append(items), nil
}

func (w *writer) ReopenReader() (*reader, int64, int64) {
	rdr := reopenReader(w.segment, w.params, w.index.reader())
	nextOffset, nextTime := w.index.getNext()
	return rdr, nextOffset, nextTime
}

var errSegmentChanged = errors.New("writing segment changed")

func (w *writer) Delete(rs *segment.RewriteSegment) (*writer, *reader, error) {
	if err := w.Sync(); err != nil {
		return nil, nil, fmt.Errorf("[klevdb.writer.Delete] sync: %w", err)
	}

	if len(rs.SurviveOffsets)+len(rs.DeletedOffsets) != w.index.Len() {
		// the number of messages changed, nothing to drop
		if err := rs.Segment.Remove(); err != nil {
			return nil, nil, fmt.Errorf("[klevdb.writer.Delete] rewrite remove: %w", err)
		}
		return nil, nil, fmt.Errorf("[klevdb.writer.Delete] rewrite check: %w", errSegmentChanged)
	}

	if err := w.Close(); err != nil {
		return nil, nil, fmt.Errorf("[klevdb.writer.Delete] close: %w", err)
	}

	if len(rs.SurviveOffsets) == 0 {
		if err := rs.Segment.Remove(); err != nil {
			return nil, nil, err
		}

		nextOffset, nextTime := w.index.getNext()
		nseg := segment.New(w.segment.Dir, nextOffset)
		nwrt, err := openWriter(nseg, w.params, nextTime)
		if err != nil {
			return nil, nil, err
		}

		if err := w.segment.Remove(); err != nil {
			return nil, nil, err
		}

		return nwrt, nil, nil
	}

	nseg := rs.GetNewSegment()
	if nseg != w.segment {
		// the starting offset of the new segment is different
		if err := rs.Segment.Rename(nseg); err != nil {
			return nil, nil, err
		}

		if err := w.segment.Remove(); err != nil {
			return nil, nil, err
		}

		// first move the replacement
		nextOffset, nextTime := w.index.getNext()
		if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
			rdr := openReader(nseg, w.params, false)
			wrt, err := openWriter(segment.New(w.segment.Dir, nextOffset), w.params, nextTime)
			return wrt, rdr, err
		} else {
			wrt, err := openWriter(nseg, w.params, nextTime)
			return wrt, nil, err
		}
	}

	if err := rs.Segment.Override(w.segment); err != nil {
		return nil, nil, err
	}

	nextOffset, nextTime := w.index.getNext()
	if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
		rdr := openReader(w.segment, w.params, false)
		wrt, err := openWriter(segment.New(w.segment.Dir, nextOffset), w.params, nextTime)
		return wrt, rdr, err
	} else {
		wrt, err := openWriter(w.segment, w.params, nextTime)
		return wrt, nil, err
	}
}

func (w *writer) Sync() error {
	if err := w.messages.Sync(); err != nil {
		return fmt.Errorf("[klevdb.writer.Sync] messages: %w", err)
	}
	if err := w.items.Sync(); err != nil {
		return fmt.Errorf("[klevdb.writer.Sync] index: %w", err)
	}
	return nil
}

func (w *writer) Close() error {
	if err := w.messages.Close(); err != nil {
		return fmt.Errorf("[klevdb.writer.Close] messages: %w", err)
	}
	if err := w.items.Close(); err != nil {
		return fmt.Errorf("[klevdb.writer.Close] index: %w", err)
	}

	return w.reader.Close()
}

type writerIndex struct {
	items      []index.Item
	keys       art.Tree
	nextOffset atomic.Int64
	nextTime   atomic.Int64

	mu sync.RWMutex
}

func newWriterIndex(items []index.Item, hasKeys bool, offset int64, timestamp int64) *writerIndex {
	var keys art.Tree
	if hasKeys {
		keys = art.New()
		index.AppendKeys(keys, items)
	}

	ix := &writerIndex{
		items: items,
		keys:  keys,
	}

	nextOffset := offset
	nextTime := timestamp
	if len(items) > 0 {
		nextOffset = items[len(items)-1].Offset + 1
		nextTime = items[len(items)-1].Timestamp
	}
	ix.nextOffset.Store(nextOffset)
	ix.nextTime.Store(nextTime)

	return ix
}

func (ix *writerIndex) GetNextOffset() (int64, error) {
	return ix.nextOffset.Load(), nil
}

func (ix *writerIndex) getNext() (int64, int64) {
	return ix.nextOffset.Load(), ix.nextTime.Load()
}

func (ix *writerIndex) getLastOffset() int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return ix.items[len(ix.items)-1].Offset
}

func (ix *writerIndex) append(items []index.Item) int64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	ix.items = append(ix.items, items...)
	if ix.keys != nil {
		index.AppendKeys(ix.keys, items)
	}
	if ln := len(items); ln > 0 {
		ix.nextTime.Store(items[ln-1].Timestamp)
		ix.nextOffset.Store(items[ln-1].Offset + 1)
	}
	return ix.nextOffset.Load()
}

func (ix *writerIndex) reader() *readerIndex {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return &readerIndex{ix.items, ix.keys, ix.nextOffset.Load(), false}
}

func (ix *writerIndex) Consume(offset int64) (int64, int64, int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, maxPosition, err := index.Consume(ix.items, offset)
	switch {
	case err == nil:
		return position, maxPosition, offset, nil
	case errors.Is(err, index.ErrIndexEmpty):
		if nextOffset := ix.nextOffset.Load(); offset <= nextOffset {
			return -1, -1, nextOffset, nil
		}
	case errors.Is(err, message.ErrInvalidOffset):
		if nextOffset := ix.nextOffset.Load(); offset == nextOffset {
			return -1, -1, nextOffset, nil
		}
	}
	return -1, -1, OffsetInvalid, fmt.Errorf("[klevdb.writerIndex.Consume] consume: %w", err)
}

func (ix *writerIndex) Get(offset int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, err := index.Get(ix.items, offset)
	switch {
	case err == nil:
		return position, nil
	case errors.Is(err, message.ErrNotFound):
		if nextOffset := ix.nextOffset.Load(); offset >= nextOffset {
			return -1, fmt.Errorf("[klevdb.writerIndex.Get] get offset too big: %w", message.ErrInvalidOffset)
		}
	}
	return -1, fmt.Errorf("[klevdb.writerIndex.Get] get: %w", err)
}

func (ix *writerIndex) Keys(keyHash []byte) ([]int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	offsets, err := index.Keys(ix.keys, keyHash)
	if err != nil {
		return nil, fmt.Errorf("[klevdb.writerIndex.Keys] keys: %w", err)
	}
	return offsets, nil
}

func (ix *writerIndex) Time(ts int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	offset, err := index.Time(ix.items, ts)
	if err != nil {
		return OffsetInvalid, fmt.Errorf("[klevdb.writerIndex.Time] time: %w", err)
	}
	return offset, nil
}

func (ix *writerIndex) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return len(ix.items)
}
