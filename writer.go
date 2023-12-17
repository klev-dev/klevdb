package klevdb

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
	"github.com/klev-dev/kleverr"
)

type writer[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	segment segment.Segment[IX, IT, IC, IS]
	ix      IX

	messages *message.Writer
	items    *index.Writer[IX, IT, IC, IS]
	index    *writerIndex[IX, IT, IC, IS]
	reader   *reader[IX, IT, IC, IS]
}

func openWriter[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](seg segment.Segment[IX, IT, IC, IS], ix IX, nextContext IC) (*writer[IX, IT, IC, IS], error) {
	messages, err := message.OpenWriter(seg.Log)
	if err != nil {
		return nil, err
	}

	var wix *writerIndex[IX, IT, IC, IS]
	if messages.Size() > 0 {
		indexItems, err := seg.ReindexAndReadIndex(ix)
		if err != nil {
			return nil, err
		}
		wix = newWriterIndex(ix, indexItems, seg.Offset, nextContext)
	} else {
		wix = newWriterIndex[IX, IT, IC, IS](ix, nil, seg.Offset, nextContext)
	}

	items, err := index.OpenWriter[IX, IT, IC, IS](seg.Index, ix)
	if err != nil {
		return nil, err
	}

	reader, err := openReaderAppend(seg, ix, wix)
	if err != nil {
		return nil, err
	}

	return &writer[IX, IT, IC, IS]{
		segment: seg,
		ix:      ix,

		messages: messages,
		items:    items,
		index:    wix,
		reader:   reader,
	}, nil
}

func (w *writer[IX, IT, IC, IS]) GetNextOffset() (int64, error) {
	return w.index.GetNextOffset()
}

func (w *writer[IX, IT, IC, IS]) NeedsRollover(rollover int64) bool {
	return (w.messages.Size() + w.items.Size()) > rollover
}

func (w *writer[IX, IT, IC, IS]) Publish(msgs []message.Message) (int64, error) {
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

func (w *writer[IX, IT, IC, IS]) ReopenReader() (*reader[IX, IT, IC, IS], int64, IC) {
	rdr := reopenReader(w.segment, w.ix, w.index.reader())
	nextOffset, nextContext := w.index.getNext()
	return rdr, nextOffset, nextContext
}

var errSegmentChanged = errors.New("writing segment changed")

func (w *writer[IX, IT, IC, IS]) Delete(rs *segment.RewriteSegment[IX, IT, IC, IS]) (*writer[IX, IT, IC, IS], *reader[IX, IT, IC, IS], error) {
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
		nseg := segment.New[IX, IT, IC, IS](w.segment.Dir, nextOffset)
		nwrt, err := openWriter(nseg, w.ix, nextContext)
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
			rdr := openReader(nseg, w.ix, false)
			wrt, err := openWriter(segment.New[IX, IT, IC](w.segment.Dir, nextOffset), w.ix, nextTime)
			return wrt, rdr, err
		} else {
			wrt, err := openWriter(nseg, w.ix, nextTime)
			return wrt, nil, err
		}
	}

	if err := rs.Segment.Override(w.segment); err != nil {
		return nil, nil, err
	}

	nextOffset, nextTime := w.index.getNext()
	if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
		rdr := openReader(w.segment, w.ix, false)
		wrt, err := openWriter(segment.New[IX, IT, IC](w.segment.Dir, nextOffset), w.ix, nextTime)
		return wrt, rdr, err
	} else {
		wrt, err := openWriter(w.segment, w.ix, nextTime)
		return wrt, nil, err
	}
}

func (w *writer[IX, IT, IC, IS]) Sync() error {
	if err := w.messages.Sync(); err != nil {
		return err
	}
	if err := w.items.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *writer[IX, IT, IC, IS]) Close() error {
	if err := w.messages.Close(); err != nil {
		return err
	}
	if err := w.items.Close(); err != nil {
		return err
	}

	return w.reader.Close()
}

type writerIndex[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	ix          IX
	store       IS
	nextOffset  atomic.Int64
	nextContext atomicValue[IC]

	mu sync.RWMutex
}

func newWriterIndex[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](ix IX, items []IT, offset int64, context IC) *writerIndex[IX, IT, IC, IS] {
	wix := &writerIndex[IX, IT, IC, IS]{
		ix:    ix,
		store: ix.NewStore(items),
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

func (wix *writerIndex[IX, IT, IC, IS]) GetNextOffset() (int64, error) {
	return wix.nextOffset.Load(), nil
}

func (wix *writerIndex[IX, IT, IC, IS]) getNext() (int64, IC) {
	return wix.nextOffset.Load(), wix.nextContext.Load()
}

func (wix *writerIndex[IX, IT, IC, IS]) getLastOffset() int64 {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.store.GetLastOffset()
}

func (wix *writerIndex[IX, IT, IC, IS]) append(items []IT) int64 {
	wix.mu.Lock()
	defer wix.mu.Unlock()

	wix.ix.Append(wix.store, items)
	if ln := len(items); ln > 0 {
		wix.nextContext.Store(wix.ix.Context(items[ln-1]))
		wix.nextOffset.Store(items[ln-1].Offset() + 1)
	}
	return wix.nextOffset.Load()
}

func (wix *writerIndex[IX, IT, IC, IS]) reader() *readerIndex[IX, IT, IC, IS] {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return &readerIndex[IX, IT, IC, IS]{wix.store, wix.nextOffset.Load(), false}
}

func (wix *writerIndex[IX, IT, IC, IS]) Consume(offset int64) (int64, int64, int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	position, maxPosition, err := wix.store.Consume(offset)
	if err == index.ErrIndexEmpty {
		if nextOffset := wix.nextOffset.Load(); offset <= nextOffset {
			return -1, -1, nextOffset, nil
		}
	} else if err == message.ErrInvalidOffset {
		if nextOffset := wix.nextOffset.Load(); offset == nextOffset {
			return -1, -1, nextOffset, nil
		}
	}
	return position, maxPosition, offset, err
}

func (wix *writerIndex[IX, IT, IC, IS]) Get(offset int64) (int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	position, err := wix.store.Get(offset)
	if err == message.ErrNotFound {
		if nextOffset := wix.nextOffset.Load(); offset >= nextOffset {
			return 0, message.ErrInvalidOffset
		}
	}
	return position, err
}

func (wix *writerIndex[IX, IT, IC, IS]) Keys(keyHash []byte) ([]int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.store.Keys(keyHash)
}

func (wix *writerIndex[IX, IT, IC, IS]) Time(ts int64) (int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.store.Time(ts)
}

func (wix *writerIndex[IX, IT, IC, IS]) Len() int {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.store.Len()
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
