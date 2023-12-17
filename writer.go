package klevdb

import (
	"errors"
	"sync"
	"time"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
	"github.com/klev-dev/kleverr"
)

type writer[IX index.Index[IT, IS, IR], IT index.Item, IS index.State, IR index.Runtime] struct {
	segment segment.Segment[IX, IT, IS, IR]
	ix      IX

	messages *message.Writer
	items    *index.Writer[IX, IT, IS, IR]
	index    *writerIndex[IX, IT, IS, IR]
	reader   *reader[IX, IT, IS, IR]
}

func openWriter[IX index.Index[IT, IS, IR], IT index.Item, IS index.State, IR index.Runtime](seg segment.Segment[IX, IT, IS, IR], ix IX, nextContext IS) (*writer[IX, IT, IS, IR], error) {
	messages, err := message.OpenWriter(seg.Log)
	if err != nil {
		return nil, err
	}

	var wix *writerIndex[IX, IT, IS, IR]
	if messages.Size() > 0 {
		indexItems, err := seg.ReindexAndReadIndex(ix)
		if err != nil {
			return nil, err
		}
		wix = newWriterIndex(ix, indexItems, seg.Offset, nextContext)
	} else {
		wix = newWriterIndex[IX, IT, IS, IR](ix, nil, seg.Offset, nextContext)
	}

	items, err := index.OpenWriter[IX, IT, IS, IR](seg.Index, ix)
	if err != nil {
		return nil, err
	}

	reader, err := openReaderAppend(seg, ix, wix)
	if err != nil {
		return nil, err
	}

	return &writer[IX, IT, IS, IR]{
		segment: seg,
		ix:      ix,

		messages: messages,
		items:    items,
		index:    wix,
		reader:   reader,
	}, nil
}

func (w *writer[IX, IT, IS, IR]) GetNextOffset() (int64, error) {
	return w.index.GetNextOffset()
}

func (w *writer[IX, IT, IS, IR]) NeedsRollover(rollover int64) bool {
	return (w.messages.Size() + w.items.Size()) > rollover
}

func (w *writer[IX, IT, IS, IR]) Publish(msgs []message.Message) (int64, error) {
	nextOffset, nextContext := w.index.getNext()

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

		items[i], nextContext, err = w.ix.New(msgs[i], position, nextContext)
		if err != nil {
			return OffsetInvalid, err
		}
		if err := w.items.Write(items[i]); err != nil {
			return OffsetInvalid, err
		}
	}

	return w.index.append(items), nil
}

func (w *writer[IX, IT, IS, IR]) ReopenReader() (*reader[IX, IT, IS, IR], int64, IS) {
	rdr := reopenReader(w.segment, w.ix, w.index.reader())
	nextOffset, nextContext := w.index.getNext()
	return rdr, nextOffset, nextContext
}

var errSegmentChanged = errors.New("writing segment changed")

func (w *writer[IX, IT, IS, IR]) Delete(rs *segment.RewriteSegment[IX, IT, IS, IR]) (*writer[IX, IT, IS, IR], *reader[IX, IT, IS, IR], error) {
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
		nseg := segment.New[IX, IT, IS, IR](w.segment.Dir, nextOffset)
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
			wrt, err := openWriter(segment.New[IX, IT, IS](w.segment.Dir, nextOffset), w.ix, nextTime)
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
		wrt, err := openWriter(segment.New[IX, IT, IS](w.segment.Dir, nextOffset), w.ix, nextTime)
		return wrt, rdr, err
	} else {
		wrt, err := openWriter(w.segment, w.ix, nextTime)
		return wrt, nil, err
	}
}

func (w *writer[IX, IT, IS, IR]) Sync() error {
	if err := w.messages.Sync(); err != nil {
		return err
	}
	if err := w.items.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *writer[IX, IT, IS, IR]) Close() error {
	if err := w.messages.Close(); err != nil {
		return err
	}
	if err := w.items.Close(); err != nil {
		return err
	}

	return w.reader.Close()
}

type writerIndex[IX index.Index[IT, IS, IR], IT index.Item, IS index.State, IR index.Runtime] struct {
	ix      IX
	runtime IR
	mu      sync.RWMutex
}

func newWriterIndex[IX index.Index[IT, IS, IR], IT index.Item, IS index.State, IR index.Runtime](ix IX, items []IT, offset int64, state IS) *writerIndex[IX, IT, IS, IR] {
	return &writerIndex[IX, IT, IS, IR]{
		ix:      ix,
		runtime: ix.NewRuntime(items, offset, state),
	}
}

func (wix *writerIndex[IX, IT, IS, IR]) GetNextOffset() (int64, error) {
	return wix.runtime.GetNextOffset(), nil
}

func (wix *writerIndex[IX, IT, IS, IR]) getNext() (int64, IS) {
	return wix.ix.Next(wix.runtime)
}

func (wix *writerIndex[IX, IT, IS, IR]) getLastOffset() int64 {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.runtime.GetLastOffset()
}

func (wix *writerIndex[IX, IT, IS, IR]) append(items []IT) int64 {
	wix.mu.Lock()
	defer wix.mu.Unlock()

	return wix.ix.Append(wix.runtime, items)
}

func (wix *writerIndex[IX, IT, IS, IR]) reader() *readerIndex[IX, IT, IS, IR] {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return &readerIndex[IX, IT, IS, IR]{wix.runtime, false}
}

func (wix *writerIndex[IX, IT, IS, IR]) Consume(offset int64) (int64, int64, int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	position, maxPosition, err := wix.runtime.Consume(offset)
	switch {
	case err == index.ErrIndexEmpty:
		if nextOffset := wix.runtime.GetNextOffset(); offset <= nextOffset {
			return -1, -1, nextOffset, nil
		}
	case err == message.ErrInvalidOffset:
		if nextOffset := wix.runtime.GetNextOffset(); offset == nextOffset {
			return -1, -1, nextOffset, nil
		}
	}
	return position, maxPosition, offset, err
}

func (wix *writerIndex[IX, IT, IS, IR]) Get(offset int64) (int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	position, err := wix.runtime.Get(offset)
	if err == message.ErrNotFound {
		if nextOffset := wix.runtime.GetNextOffset(); offset >= nextOffset {
			return 0, message.ErrInvalidOffset
		}
	}
	return position, err
}

func (wix *writerIndex[IX, IT, IS, IR]) Keys(keyHash []byte) ([]int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.runtime.Keys(keyHash)
}

func (wix *writerIndex[IX, IT, IS, IR]) Time(ts int64) (int64, error) {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.runtime.Time(ts)
}

func (wix *writerIndex[IX, IT, IS, IR]) Len() int {
	wix.mu.RLock()
	defer wix.mu.RUnlock()

	return wix.runtime.Len()
}
