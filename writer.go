package klevdb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
	"github.com/klev-dev/kleverr"
)

type writer[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext] struct {
	segment segment.Segment[IX, IT, IC]
	ix      IX
	keys    bool

	messages *message.Writer
	items    *index.Writer[IX, IT, IC]
	index    *writerIndex[IX, IT, IC]
	reader   *reader[IX, IT, IC]
}

func openWriter[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext](seg segment.Segment[IX, IT, IC], ix IX, keys bool, nextContext IC) (*writer[IX, IT, IC], error) {
	messages, err := message.OpenWriter(seg.Log)
	if err != nil {
		return nil, err
	}

	var wix *writerIndex[IX, IT, IC]
	if messages.Size() > 0 {
		indexItems, err := seg.ReindexAndReadIndex(ix)
		if err != nil {
			return nil, err
		}
		wix = newWriterIndex[IX, IT, IC](ix, indexItems, keys, seg.Offset, nextContext)
	} else {
		wix = newWriterIndex[IX, IT, IC](ix, nil, keys, seg.Offset, nextContext)
	}

	items, err := index.OpenWriter[IX, IT, IC](seg.Index, ix)
	if err != nil {
		return nil, err
	}

	reader, err := openReaderAppend(seg, ix, keys, wix)
	if err != nil {
		return nil, err
	}

	return &writer[IX, IT, IC]{
		segment: seg,
		ix:      ix,
		keys:    keys,

		messages: messages,
		items:    items,
		index:    wix,
		reader:   reader,
	}, nil
}

func (w *writer[IX, IT, IC]) GetNextOffset() (int64, error) {
	return w.index.GetNextOffset()
}

func (w *writer[IX, IT, IC]) NeedsRollover(rollover int64) bool {
	return (w.messages.Size() + w.items.Size()) > rollover
}

func (w *writer[IX, IT, IC]) Publish(msgs []message.Message) (int64, error) {
	nextOffset, nextTime := w.index.getNext()

	items := make([]IT, len(msgs))
	for i := range msgs {
		msgs[i].Offset = nextOffset + int64(i)
		if msgs[i].Time.IsZero() {
			msgs[i].Time = time.Now().UTC()
		}

		position, err := w.messages.Write(msgs[i])
		if err != nil {
			return OffsetInvalid, err
		}

		items[i], nextTime, err = w.ix.New(msgs[i], position, nextTime)
		if err != nil {
			return OffsetInvalid, err
		}
		if err := w.items.Write(items[i]); err != nil {
			return OffsetInvalid, err
		}
	}

	return w.index.append(items), nil
}

func (w *writer[IX, IT, IC]) ReopenReader() (*reader[IX, IT, IC], int64, IC) {
	rdr := reopenReader(w.segment, w.ix, w.keys, w.index.reader())
	nextOffset, nextContext := w.index.getNext()
	return rdr, nextOffset, nextContext
}

var errSegmentChanged = errors.New("writing segment changed")

func (w *writer[IX, IT, IC]) Delete(rs *segment.RewriteSegment[IX, IT, IC]) (*writer[IX, IT, IC], *reader[IX, IT, IC], error) {
	if err := w.Sync(); err != nil {
		return nil, nil, err
	}

	if len(rs.SurviveOffsets)+len(rs.DeletedOffsets) != w.index.Len() {
		// the number of messages changed, nothing to drop
		if err := rs.Segment.Remove(); err != nil {
			return nil, nil, err
		}
		return nil, nil, kleverr.Newf("delete failed: %w", errSegmentChanged)
	}

	if err := w.Close(); err != nil {
		return nil, nil, err
	}

	if len(rs.SurviveOffsets) == 0 {
		if err := rs.Segment.Remove(); err != nil {
			return nil, nil, err
		}

		nextOffset, nextContext := w.index.getNext()
		nseg := segment.New[IX, IT, IC](w.segment.Dir, nextOffset)
		nwrt, err := openWriter(nseg, w.ix, w.keys, nextContext)
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
			rdr := openReader(nseg, w.ix, w.keys, false)
			wrt, err := openWriter(segment.New[IX, IT, IC](w.segment.Dir, nextOffset), w.ix, w.keys, nextTime)
			return wrt, rdr, err
		} else {
			wrt, err := openWriter(nseg, w.ix, w.keys, nextTime)
			return wrt, nil, err
		}
	}

	if err := rs.Segment.Override(w.segment); err != nil {
		return nil, nil, err
	}

	nextOffset, nextTime := w.index.getNext()
	if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
		rdr := openReader(w.segment, w.ix, w.keys, false)
		wrt, err := openWriter(segment.New[IX, IT, IC](w.segment.Dir, nextOffset), w.ix, w.keys, nextTime)
		return wrt, rdr, err
	} else {
		wrt, err := openWriter(w.segment, w.ix, w.keys, nextTime)
		return wrt, nil, err
	}
}

func (w *writer[IX, IT, IC]) Sync() error {
	if err := w.messages.Sync(); err != nil {
		return err
	}
	if err := w.items.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *writer[IX, IT, IC]) Close() error {
	if err := w.messages.Close(); err != nil {
		return err
	}
	if err := w.items.Close(); err != nil {
		return err
	}

	return w.reader.Close()
}

type writerIndex[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext] struct {
	ix          IX
	items       []IT
	keys        art.Tree
	nextOffset  atomic.Int64
	nextContext atomicValue[IC]

	mu sync.RWMutex
}

func newWriterIndex[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext](ix IX, items []IT, hasKeys bool, offset int64, context IC) *writerIndex[IX, IT, IC] {
	var keys art.Tree
	if hasKeys {
		keys = art.New()
		index.AppendKeys(keys, items)
	}

	wix := &writerIndex[IX, IT, IC]{
		items: items,
		keys:  keys,
	}

	nextOffset := offset
	nextContext := context
	if len(items) > 0 {
		nextOffset = items[len(items)-1].Offset() + 1
		nextContext = ix.Context(items[len(items)-1])
	}
	wix.nextOffset.Store(nextOffset)
	wix.nextContext.v.Store(nextContext)

	return wix
}

func (ix *writerIndex[IX, IT, IC]) GetNextOffset() (int64, error) {
	return ix.nextOffset.Load(), nil
}

func (ix *writerIndex[IX, IT, IC]) getNext() (int64, IC) {
	return ix.nextOffset.Load(), ix.nextContext.Load()
}

func (ix *writerIndex[IX, IT, IC]) getLastOffset() int64 {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return ix.items[len(ix.items)-1].Offset()
}

func (ix *writerIndex[IX, IT, IC]) append(items []IT) int64 {
	ix.mu.Lock()
	defer ix.mu.Unlock()

	ix.items = append(ix.items, items...)
	if ix.keys != nil {
		index.AppendKeys(ix.keys, items)
	}
	if ln := len(items); ln > 0 {
		ix.nextContext.Store(ix.ix.Context(items[ln-1]))
		ix.nextOffset.Store(items[ln-1].Offset() + 1)
	}
	return ix.nextOffset.Load()
}

func (ix *writerIndex[IX, IT, IC]) reader() *readerIndex[IX, IT, IC] {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return &readerIndex[IX, IT, IC]{ix.items, ix.keys, ix.nextOffset.Load(), false}
}

func (ix *writerIndex[IX, IT, IC]) Consume(offset int64) (int64, int64, int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, maxPosition, err := index.Consume(ix.items, offset)
	if err == index.ErrIndexEmpty {
		if nextOffset := ix.nextOffset.Load(); offset <= nextOffset {
			return -1, -1, nextOffset, nil
		}
	} else if err == message.ErrInvalidOffset {
		if nextOffset := ix.nextOffset.Load(); offset == nextOffset {
			return -1, -1, nextOffset, nil
		}
	}
	return position, maxPosition, offset, err
}

func (ix *writerIndex[IX, IT, IC]) Get(offset int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, err := index.Get(ix.items, offset)
	if err == message.ErrNotFound {
		if nextOffset := ix.nextOffset.Load(); offset >= nextOffset {
			return 0, message.ErrInvalidOffset
		}
	}
	return position, err
}

func (ix *writerIndex[IX, IT, IC]) Keys(keyHash []byte) ([]int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return index.Keys(ix.keys, keyHash)
}

func (ix *writerIndex[IX, IT, IC]) Time(ts int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return index.Time(ix.items, ts)
}

func (ix *writerIndex[IX, IT, IC]) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return len(ix.items)
}

type atomicValue[T any] struct {
	v atomic.Value
}

func newAtomicValue[T any](initial T) *atomicValue[T] {
	av := &atomicValue[T]{}
	av.v.Store(initial)
	return av
}

func (v *atomicValue[T]) Store(t T) {
	v.v.Store(t)
}

func (v *atomicValue[T]) Load() T {
	return v.v.Load().(T)
}
