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

type writer struct {
	segment segment.Segment
	params  index.Params

	messages *message.Writer
	items    *index.Writer
	index    *writerIndex
	reader   *reader
}

func openWriter(seg segment.Segment, params index.Params) (*writer, error) {
	messages, err := message.OpenWriter(seg.Log)
	if err != nil {
		return nil, err
	}

	var ix *writerIndex
	if messages.Size() > 0 {
		indexItems, err := seg.ReindexAndReadIndex(params)
		if err != nil {
			return nil, err
		}
		ix = newWriterIndex(indexItems, params.Keys, seg.Offset)
	} else {
		ix = newWriterIndex(nil, params.Keys, seg.Offset)
	}

	items, err := index.OpenWriter(seg.Index, params)
	if err != nil {
		return nil, err
	}

	reader, err := openReaderAppend(seg, params, ix)
	if err != nil {
		return nil, err
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
	return w.index.GetNextOffset()
}

func (w *writer) NeedsRollover(rollover int64) bool {
	return (w.messages.Size() + w.items.Size()) > rollover
}

func (w *writer) Publish(msgs []message.Message) (int64, error) {
	nextOffset, err := w.index.GetNextOffset()
	if err != nil {
		return OffsetInvalid, err
	}

	items := make([]index.Item, len(msgs))
	for i := range msgs {
		msgs[i].Offset = nextOffset + int64(i)
		if msgs[i].Time.IsZero() {
			msgs[i].Time = time.Now().UTC()
		}

		position, err := w.messages.Write(msgs[i])
		if err != nil {
			return OffsetInvalid, err
		}

		items[i] = w.params.NewItem(msgs[i], position)
		if err := w.items.Write(items[i]); err != nil {
			return OffsetInvalid, err
		}
	}

	return w.index.append(items), nil
}

func (w *writer) ReopenReader() (*reader, int64, error) {
	rdr := reopenReader(w.segment, w.params, w.index.reader())
	nextOffset, err := w.index.GetNextOffset()
	if err != nil {
		return nil, OffsetInvalid, err
	}
	return rdr, nextOffset, nil
}

var errSegmentChanged = errors.New("writing segment changed")

func (w *writer) Delete(rs *segment.RewriteSegment) (*writer, *reader, error) {
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

		nextOffset, err := w.index.GetNextOffset()
		if err != nil {
			return nil, nil, err
		}

		nseg := segment.New(w.segment.Dir, nextOffset)
		nwrt, err := openWriter(nseg, w.params)
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
		if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
			nextOffset, err := w.index.GetNextOffset()
			if err != nil {
				return nil, nil, err
			}

			rdr := openReader(nseg, w.params, false)
			wrt, err := openWriter(segment.New(w.segment.Dir, nextOffset), w.params)
			return wrt, rdr, err
		} else {
			wrt, err := openWriter(nseg, w.params)
			return wrt, nil, err
		}
	}

	if err := rs.Segment.Override(w.segment); err != nil {
		return nil, nil, err
	}

	if _, ok := rs.DeletedOffsets[w.index.getLastOffset()]; ok {
		nextOffset, err := w.index.GetNextOffset()
		if err != nil {
			return nil, nil, err
		}

		rdr := openReader(w.segment, w.params, false)
		wrt, err := openWriter(segment.New(w.segment.Dir, nextOffset), w.params)
		return wrt, rdr, err
	} else {
		wrt, err := openWriter(w.segment, w.params)
		return wrt, nil, err
	}
}

func (w *writer) Sync() error {
	if err := w.messages.Sync(); err != nil {
		return err
	}
	if err := w.items.Sync(); err != nil {
		return err
	}
	return nil
}

func (w *writer) Close() error {
	if err := w.messages.Close(); err != nil {
		return err
	}
	if err := w.items.Close(); err != nil {
		return err
	}

	return w.reader.Close()
}

type writerIndex struct {
	items      []index.Item
	keys       art.Tree
	nextOffset atomic.Int64

	mu sync.RWMutex
}

func newWriterIndex(items []index.Item, hasKeys bool, offset int64) *writerIndex {
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
	if len(items) > 0 {
		nextOffset = items[len(items)-1].Offset + 1
	}
	ix.nextOffset.Store(nextOffset)

	return ix
}

func (ix *writerIndex) GetNextOffset() (int64, error) {
	return ix.nextOffset.Load(), nil
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
	return ix.nextOffset.Add(int64(len(items)))
}

func (ix *writerIndex) reader() *readerIndex {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return &readerIndex{ix.items, ix.keys, ix.nextOffset.Load(), false}
}

func (ix *writerIndex) Consume(offset int64) (int64, int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, err := index.Consume(ix.items, offset)
	if err != nil {
		switch {
		case errors.Is(err, index.ErrIndexEmpty):
			if nextOffset := ix.nextOffset.Load(); offset <= nextOffset {
				return -1, nextOffset, nil
			}
		case errors.Is(err, message.ErrInvalidOffset):
			if nextOffset := ix.nextOffset.Load(); offset == nextOffset {
				return -1, nextOffset, nil
			}
		}
	}
	return position, offset, err
}

func (ix *writerIndex) Get(offset int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	position, err := index.Get(ix.items, offset)
	if err != nil {
		switch {
		case errors.Is(err, message.ErrNotFound):
			if nextOffset := ix.nextOffset.Load(); offset >= nextOffset {
				return 0, kleverr.Newf("%w: offset %d after end of log", message.ErrInvalidOffset, offset)
			}
		}
	}
	return position, err
}

func (ix *writerIndex) Keys(keyHash []byte) ([]int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return index.Keys(ix.keys, keyHash)
}

func (ix *writerIndex) Time(ts int64) (int64, error) {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return index.Time(ix.items, ts)
}

func (ix *writerIndex) Len() int {
	ix.mu.RLock()
	defer ix.mu.RUnlock()

	return len(ix.items)
}
