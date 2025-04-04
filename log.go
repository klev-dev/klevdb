package klevdb

import (
	"errors"
	"fmt"
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
)

var errNoKeyIndex = fmt.Errorf("%w by key", ErrNoIndex)
var errKeyNotFound = fmt.Errorf("key %w", message.ErrNotFound)
var errNoTimeIndex = fmt.Errorf("%w by time", ErrNoIndex)
var errTimeNotFound = fmt.Errorf("time %w", message.ErrNotFound)
var errDeleteRelative = fmt.Errorf("%w: delete relative offsets", message.ErrInvalidOffset)

// Open create a log based on a dir and set of options
func Open(dir string, opts Options) (result Log, err error) {
	if opts.Rollover == 0 {
		opts.Rollover = 1024 * 1024
	}

	if opts.CreateDirs {
		if err := os.MkdirAll(dir, 0700); err != nil {
			return nil, fmt.Errorf("open create dirs: %w", err)
		}
	}

	lock := flock.New(filepath.Join(dir, ".lock"))
	if opts.Readonly {
		switch ok, err := lock.TryRLock(); {
		case err != nil:
			return nil, fmt.Errorf("open read lock: %w", err)
		case !ok:
			return nil, fmt.Errorf("open already writing locked")
		}
	} else {
		switch ok, err := lock.TryLock(); {
		case err != nil:
			return nil, fmt.Errorf("open lock: %w", err)
		case !ok:
			return nil, fmt.Errorf("open already locked")
		}
	}
	defer func() {
		if err != nil {
			if lerr := lock.Unlock(); lerr != nil {
				err = fmt.Errorf("%w: open release lock: %w", err, lerr)
			}
		}
	}()

	params := index.Params{Times: opts.TimeIndex, Keys: opts.KeyIndex}

	l := &log{
		dir:    dir,
		opts:   opts,
		params: params,
		lock:   lock,
	}

	segments, err := segment.Find(dir)
	if err != nil {
		return nil, err
	}

	if len(segments) == 0 {
		if opts.Readonly {
			ix := newReaderIndex(nil, params.Keys, 0, true)
			rdr := reopenReader(segment.New(dir, 0), params, ix)
			l.readers = []*reader{rdr}
		} else {
			w, err := openWriter(segment.New(dir, 0), params, 0)
			if err != nil {
				return nil, err
			}
			l.writer = w
			l.readers = []*reader{w.reader}
		}
	} else {
		head := segments[len(segments)-1]
		if opts.Check {
			if err := head.Check(params); err != nil {
				return nil, err
			}
		}

		for _, seg := range segments[:len(segments)-1] {
			rdr := openReader(seg, params, false)
			l.readers = append(l.readers, rdr)
		}

		if opts.Readonly {
			rdr := openReader(head, params, true)
			l.readers = append(l.readers, rdr)
		} else {
			wrt, err := openWriter(head, params, 0)
			if err != nil {
				return nil, err
			}
			l.writer = wrt
			l.readers = append(l.readers, wrt.reader)
		}
	}

	return l, nil
}

type log struct {
	dir    string
	opts   Options
	params index.Params
	lock   *flock.Flock

	writer   *writer
	writerMu sync.Mutex

	readers   []*reader
	readersMu sync.RWMutex

	deleteMu sync.Mutex
}

func (l *log) Publish(msgs []message.Message) (int64, error) {
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
		newWriter, err := openWriter(segment.New(l.dir, nextOffset), l.params, nextTime)
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

func (l *log) NextOffset() (int64, error) {
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

func (l *log) Consume(offset int64, maxCount int64) (int64, []message.Message, error) {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, segmentIndex := segment.Consume(l.readers, offset)

	nextOffset, msgs, err := rdr.Consume(offset, maxCount)
	if err == index.ErrOffsetAfterEnd && segmentIndex < len(l.readers)-1 {
		// this is after the end, consume starting the next one
		next := l.readers[segmentIndex+1]
		return next.Consume(message.OffsetOldest, maxCount)
	}

	return nextOffset, msgs, err
}

func (l *log) ConsumeByKey(key []byte, offset int64, maxCount int64) (int64, []message.Message, error) {
	if !l.opts.KeyIndex {
		return OffsetInvalid, nil, errNoKeyIndex
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

func (l *log) Get(offset int64) (message.Message, error) {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, segmentIndex, err := segment.Get(l.readers, offset)
	if err != nil {
		return message.Invalid, err
	}

	msg, err := rdr.Get(offset)
	if err == index.ErrOffsetAfterEnd && segmentIndex < len(l.readers)-1 {
		return msg, index.ErrOffsetNotFound
	}
	return msg, err
}

func (l *log) GetByKey(key []byte) (message.Message, error) {
	if !l.opts.KeyIndex {
		return message.Invalid, errNoKeyIndex
	}

	hash := index.KeyHashEncoded(index.KeyHash(key))
	tctx := time.Now().UnixMicro()

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	for i := len(l.readers) - 1; i >= 0; i-- {
		rdr := l.readers[i]

		switch msg, err := rdr.GetByKey(key, hash, tctx); {
		case err == nil:
			return msg, nil
		case err == index.ErrKeyNotFound:
			// not in this segment, try the rest
		default:
			return message.Invalid, err
		}
	}

	// not in any segment, so just return the error
	return message.Invalid, errKeyNotFound
}

func (l *log) OffsetByKey(key []byte) (int64, error) {
	msg, err := l.GetByKey(key)
	if err != nil {
		return OffsetInvalid, err
	}
	return msg.Offset, nil
}

func (l *log) GetByTime(start time.Time) (message.Message, error) {
	if !l.opts.TimeIndex {
		return message.Invalid, errNoTimeIndex
	}

	ts := start.UnixMicro()
	tctx := time.Now().UnixMicro()

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	for i := len(l.readers) - 1; i >= 0; i-- {
		rdr := l.readers[i]

		switch msg, err := rdr.GetByTime(ts, tctx); {
		case err == nil:
			return msg, nil
		case err == index.ErrTimeBeforeStart:
			// not in this segment, try the rest
			if i == 0 {
				return rdr.Get(message.OffsetOldest)
			}
		case err == index.ErrTimeAfterEnd:
			// time is between end of this and begin next
			if i < len(l.readers)-1 {
				nextRdr := l.readers[i+1]
				return nextRdr.Get(message.OffsetOldest)
			}
			return message.Invalid, errTimeNotFound
		default:
			return message.Invalid, err
		}
	}

	return message.Invalid, errTimeNotFound
}

func (l *log) OffsetByTime(start time.Time) (int64, time.Time, error) {
	msg, err := l.GetByTime(start)
	if err != nil {
		return OffsetInvalid, time.Time{}, err
	}
	return msg.Offset, msg.Time, nil
}

func (l *log) Delete(offsets map[int64]struct{}) (map[int64]struct{}, int64, error) {
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

	rs, err := rdr.segment.Rewrite(offsets, l.params)
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
		case err == errSegmentChanged:
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

	var newReaders []*reader
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

func (l *log) findDeleteReader(offsets map[int64]struct{}) (*reader, error) {
	orderedOffsets := maps.Keys(offsets)
	slices.Sort(orderedOffsets)
	lowestOffset := orderedOffsets[0]

	if lowestOffset < 0 {
		return nil, errDeleteRelative
	}

	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	rdr, _, err := segment.Get(l.readers, lowestOffset)
	return rdr, err
}

func (l *log) Size(m message.Message) int64 {
	return message.Size(m) + l.params.Size()
}

func (l *log) Stat() (segment.Stats, error) {
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

func (l *log) Backup(dir string) error {
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

func (l *log) Sync() (int64, error) {
	if l.opts.Readonly {
		l.readersMu.RLock()
		defer l.readersMu.RUnlock()

		rdr := l.readers[len(l.readers)-1]
		return rdr.GetNextOffset()
	}

	l.writerMu.Lock()
	defer l.writerMu.Unlock()

	if err := l.writer.Sync(); err != nil {
		return OffsetInvalid, nil
	}
	return l.writer.GetNextOffset()
}

func (l *log) GC(unusedFor time.Duration) error {
	l.readersMu.RLock()
	defer l.readersMu.RUnlock()

	for _, reader := range l.readers {
		if err := reader.GC(unusedFor); err != nil {
			return err
		}
	}

	return nil
}

func (l *log) Close() error {
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
		return fmt.Errorf("close unlock: %w", err)
	}

	return nil
}
