package klevdb

import (
	"errors"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/gofrs/flock"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
	"github.com/klev-dev/kleverr"
)

// Open create a log based on a dir and set of options
func Open(dir string, opts Options) (Log, error) {
	if opts.TimeIndex && opts.KeyIndex {
		return open(dir, opts, index.TimeKeyIndex{})
	} else if opts.TimeIndex {
		return open(dir, opts, index.TimeIndex{})
	} else if opts.KeyIndex {
		return open(dir, opts, index.KeyIndex{})
	}
	return open(dir, opts, index.NoIndex{})
}

func open[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext](dir string, opts Options, ix IX) (Log, error) {
	if opts.Rollover == 0 {
		opts.Rollover = 1024 * 1024
	}

	if opts.CreateDirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, kleverr.Newf("could not create log dirs: %w", err)
		}
	}

	lock := flock.New(filepath.Join(dir, ".lock"))
	if opts.Readonly {
		switch ok, err := lock.TryRLock(); {
		case err != nil:
			return nil, kleverr.Newf("could not lock: %w", err)
		case !ok:
			return nil, kleverr.Newf("log already writing locked")
		}
	} else {
		switch ok, err := lock.TryLock(); {
		case err != nil:
			return nil, kleverr.Newf("could not lock: %w", err)
		case !ok:
			return nil, kleverr.Newf("log already locked")
		}
	}

	l := &log[IX, IT, IC]{
		dir:  dir,
		opts: opts,
		ix:   ix,
		lock: lock,
	}

	segments, err := segment.Find[IX, IT, IC](dir)
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		if opts.Readonly {
			rix := newReaderIndex[IX](nil, opts.KeyIndex, 0, true)
			rdr := reopenReader(segment.New[IX, IT, IC](dir, 0), ix, opts.KeyIndex, rix)
			l.readers = []*reader[IX, IT, IC]{rdr}
		} else {
			w, err := openWriter(segment.New[IX, IT, IC](dir, 0), ix, opts.KeyIndex, ix.NewContext())
			if err != nil {
				return nil, err
			}
			l.writer = w
			l.readers = []*reader[IX, IT, IC]{w.reader}
		}
	} else {
		head := segments[len(segments)-1]
		if opts.Check {
			if err := head.Check(ix); err != nil {
				return nil, err
			}
		}

		for _, seg := range segments[:len(segments)-1] {
			rdr := openReader(seg, ix, opts.KeyIndex, false)
			l.readers = append(l.readers, rdr)
		}

		if opts.Readonly {
			rdr := openReader(head, ix, opts.KeyIndex, true)
			l.readers = append(l.readers, rdr)
		} else {
			wrt, err := openWriter(head, ix, opts.KeyIndex, ix.NewContext())
			if err != nil {
				return nil, err
			}
			l.writer = wrt
			l.readers = append(l.readers, wrt.reader)
		}
	}

	return l, nil
}

type log[IX index.Index[IT, IC], IT index.IndexItem, IC index.IndexContext] struct {
	dir  string
	opts Options
	ix   IX
	lock *flock.Flock

	writer   *writer[IX, IT, IC]
	writerMu sync.Mutex

	readers   []*reader[IX, IT, IC]
	readersMu sync.RWMutex

	deleteMu sync.Mutex
}

func (l *log[IX, IT, IC]) Publish(msgs []message.Message) (int64, error) {
	if l.opts.Readonly {
		return OffsetInvalid, ErrReadonly
	}

	l.writerMu.Lock()
	defer l.writerMu.Unlock()

	if l.writer.NeedsRollover(l.opts.Rollover) {
		oldWriter := l.writer
		if err := oldWriter.Sync(); err != nil {
			return OffsetInvalid, err
		}

		oldReader, nextOffset, nextTime := l.writer.ReopenReader()
		newWriter, err := openWriter(segment.New[IX, IT, IC](l.dir, nextOffset), l.ix, l.opts.KeyIndex, nextTime)
		if err != nil {
			return OffsetInvalid, err
		}

		l.readersMu.Lock()

		l.readers[len(l.readers)-1] = oldReader
		l.writer = newWriter
		l.readers = append(l.readers, newWriter.reader)

		l.readersMu.Unlock()

		if err := oldWriter.Close(); err != nil {
			return OffsetInvalid, err
		}
	}

	nextOffset, err := l.writer.Publish(msgs)
	if err != nil {
		return OffsetInvalid, err
	}

	if l.opts.AutoSync {
		if err := l.writer.Sync(); err != nil {
			return OffsetInvalid, err
		}
	}

	return nextOffset, nil
}

func (l *log[IX, IT, IC]) NextOffset() (int64, error) {
	if l.opts.Readonly {
		l.readersMu.RLock()
		defer l.readersMu.RUnlock()

		rdr := l.readers[len(l.readers)-1]
		return rdr.GetNextOffset()
	}

	l.writerMu.Lock()
	defer l.writerMu.Unlock()

	return l.writer.GetNextOffset()
}

func (l *log[IX, IT, IC]) Consume(offset int64, maxCount int64) (int64, []message.Message, error) {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, index := segment.Consume(l.readers, offset)

	nextOffset, msgs, err := rdr.Consume(offset, maxCount)
	if err != nil && err == message.ErrInvalidOffset {
		if index < len(l.readers)-1 {
			// this is after the end, consume starting the next one
			next := l.readers[index+1]
			return next.Consume(message.OffsetOldest, maxCount)
		}
	}

	return nextOffset, msgs, err
}

func (l *log[IX, IT, IC]) ConsumeByKey(key []byte, offset int64, maxCount int64) (int64, []message.Message, error) {
	if !l.opts.KeyIndex {
		return OffsetInvalid, nil, kleverr.Newf("%w by key", ErrNoIndex)
	}

	hash := index.KeyHashEncoded(index.KeyHash(key))

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, index := segment.Consume(l.readers, offset)
	for {
		nextOffset, msgs, err := rdr.ConsumeByKey(key, hash, offset, maxCount)
		if err != nil {
			return nextOffset, msgs, err
		}
		if len(msgs) > 0 {
			return nextOffset, msgs, err
		}
		if index >= len(l.readers)-1 {
			return nextOffset, msgs, err
		}

		index += 1
		rdr = l.readers[index]
		offset = message.OffsetOldest
	}
}

func (l *log[IX, IT, IC]) Get(offset int64) (message.Message, error) {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, _, err := segment.Get(l.readers, offset)
	if err != nil {
		return message.Invalid, err
	}

	return rdr.Get(offset)
}

func (l *log[IX, IT, IC]) GetByKey(key []byte) (message.Message, error) {
	if !l.opts.KeyIndex {
		return message.Invalid, kleverr.Newf("%w by key", ErrNoIndex)
	}

	hash := index.KeyHashEncoded(index.KeyHash(key))

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	for i := len(l.readers) - 1; i >= 0; i-- {
		rdr := l.readers[i]

		switch msg, err := rdr.GetByKey(key, hash); {
		case err == nil:
			return msg, nil
		case err == message.ErrNotFound:
			// not in this segment, try the rest
		default:
			return message.Invalid, err
		}
	}

	// not in any segment, so just return the error
	return message.Invalid, kleverr.Newf("key %w", message.ErrNotFound)
}

func (l *log[IX, IT, IC]) OffsetByKey(key []byte) (int64, error) {
	msg, err := l.GetByKey(key)
	if err != nil {
		return OffsetInvalid, err
	}
	return msg.Offset, nil
}

func (l *log[IX, IT, IC]) GetByTime(start time.Time) (message.Message, error) {
	if !l.opts.TimeIndex {
		return message.Invalid, kleverr.Newf("%w by time", ErrNoIndex)
	}

	ts := start.UnixMicro()

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	for i := len(l.readers) - 1; i >= 0; i-- {
		rdr := l.readers[i]

		switch msg, err := rdr.GetByTime(ts); {
		case err == nil:
			return msg, nil
		case err == message.ErrInvalidOffset:
			// not in this segment, try the rest
			if i == 0 {
				return rdr.Get(message.OffsetOldest)
			}
		case err == message.ErrNotFound:
			// time is between end of this and begin next
			if i < len(l.readers)-1 {
				nextRdr := l.readers[i+1]
				return nextRdr.Get(message.OffsetOldest)
			}

			return message.Invalid, err
		default:
			return message.Invalid, err
		}
	}

	return message.Invalid, kleverr.Newf("time %w", message.ErrNotFound)
}

func (l *log[IX, IT, IC]) OffsetByTime(start time.Time) (int64, time.Time, error) {
	msg, err := l.GetByTime(start)
	if err != nil {
		return OffsetInvalid, time.Time{}, err
	}
	return msg.Offset, msg.Time, nil
}

func (l *log[IX, IT, IC]) Delete(offsets map[int64]struct{}) (map[int64]struct{}, int64, error) {
	if l.opts.Readonly {
		return nil, 0, ErrReadonly
	}

	if len(offsets) == 0 {
		return nil, 0, nil
	}

	l.deleteMu.Lock()
	defer l.deleteMu.Unlock()

	rdr, err := l.findDeleteReader(offsets)
	if err != nil {
		return nil, 0, err
	}

	l.writerMu.Lock()
	if l.writer.reader == rdr {
		if err := l.writer.Sync(); err != nil {
			l.writerMu.Unlock()
			return nil, 0, err
		}
	}
	l.writerMu.Unlock()

	rs, err := rdr.segment.Rewrite(offsets, l.ix)
	if err != nil {
		return nil, 0, err
	}

	if len(rs.DeletedOffsets) == 0 {
		// deleted nothing, just remove rewrite files
		return nil, 0, rs.Segment.Remove()
	}

	// check if we are deleting in the writing segment
	l.writerMu.Lock()
	if l.writer.reader == rdr {
		defer l.writerMu.Unlock()

		l.readersMu.Lock()
		defer l.readersMu.Unlock()

		newWriter, newReader, err := l.writer.Delete(rs)
		switch {
		case errors.Is(err, errSegmentChanged):
			return nil, 0, nil
		case err != nil:
			return nil, 0, err
		}

		l.writer = newWriter
		if newReader == nil {
			l.readers[len(l.readers)-1] = newWriter.reader
		} else {
			l.readers[len(l.readers)-1] = newReader
			l.readers = append(l.readers, newWriter.reader)
		}

		return rs.DeletedOffsets, rs.DeletedSize, nil
	}
	l.writerMu.Unlock()

	// we are deleting in a reader segment
	l.readersMu.Lock()
	defer l.readersMu.Unlock()

	newReader, err := rdr.Delete(rs)
	if err != nil {
		return nil, 0, err
	}

	var newReaders []*reader[IX, IT, IC]
	for _, r := range l.readers {
		if r.segment == rdr.segment {
			if newReader != nil {
				newReaders = append(newReaders, newReader)
			}
		} else {
			newReaders = append(newReaders, r)
		}
	}
	l.readers = newReaders

	return rs.DeletedOffsets, rs.DeletedSize, nil
}

func (l *log[IX, IT, IC]) findDeleteReader(offsets map[int64]struct{}) (*reader[IX, IT, IC], error) {
	orderedOffsets := maps.Keys(offsets)
	slices.Sort(orderedOffsets)
	lowestOffset := orderedOffsets[0]

	if lowestOffset < 0 {
		return nil, kleverr.Newf("%w: cannot delete relative offsets", message.ErrInvalidOffset)
	}

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, _, err := segment.Get(l.readers, lowestOffset)
	return rdr, err
}

func (l *log[IX, IT, IC]) Size(m message.Message) int64 {
	return message.Size(m) + l.ix.Size()
}

func (l *log[IX, IT, IC]) Stat() (segment.Stats, error) {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	if l.opts.Readonly && len(l.readers) == 1 {
		segStats, err := l.readers[0].Stat()
		if err != nil && errors.Is(err, os.ErrNotExist) {
			return segment.Stats{}, nil
		}
		return segStats, err
	}

	stats := segment.Stats{}
	for _, reader := range l.readers {
		segStats, err := reader.Stat()
		if err != nil {
			return segment.Stats{}, err
		}

		stats.Segments += segStats.Segments
		stats.Messages += segStats.Messages
		stats.Size += segStats.Size
	}
	return stats, nil
}

func (l *log[IX, IT, IC]) Backup(dir string) error {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	if l.opts.Readonly && len(l.readers) == 1 {
		err := l.readers[0].Backup(dir)
		if err != nil && errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}

	for _, reader := range l.readers {
		if err := reader.Backup(dir); err != nil {
			return err
		}
	}

	return nil
}

func (l *log[IX, IT, IC]) Sync() error {
	if l.opts.Readonly {
		return nil
	}

	l.writerMu.Lock()
	defer l.writerMu.Unlock()

	return l.writer.Sync()
}

func (l *log[IX, IT, IC]) Close() error {
	if l.opts.Readonly {
		l.readersMu.Lock()
		defer l.readersMu.Unlock()

		for _, reader := range l.readers {
			if err := reader.Close(); err != nil {
				return err
			}
		}
	} else {
		l.writerMu.Lock()
		defer l.writerMu.Unlock()

		l.readersMu.Lock()
		defer l.readersMu.Unlock()

		if err := l.writer.Sync(); err != nil {
			return err
		}

		if err := l.writer.Close(); err != nil {
			return err
		}

		for _, reader := range l.readers[:len(l.readers)-1] {
			if err := reader.Close(); err != nil {
				return err
			}
		}
	}

	if err := l.lock.Unlock(); err != nil {
		return kleverr.Newf("could not release lock: %w", err)
	}

	return nil
}
