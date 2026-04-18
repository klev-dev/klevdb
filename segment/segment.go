package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/pkg/kdir"
)

type Segment struct {
	Dir    string
	Offset int64

	Data  string
	Index string

	DataFormat  message.DetectedFormat
	IndexFormat index.DetectedFormat
}

func (s Segment) GetOffset() int64 {
	return s.Offset
}

func New(dir string, offset int64, format message.DetectedFormat) Segment {
	s := Segment{Dir: dir, Offset: offset}
	if format == message.FormatSegment {
		s.Data = filepath.Join(dir, fmt.Sprintf("%020d.segment", offset))
		s.Index = filepath.Join(dir, fmt.Sprintf("%020d.index", offset))
		s.DataFormat = message.FormatSegment
		s.IndexFormat = index.FormatSegment
	} else {
		s.Data = filepath.Join(dir, fmt.Sprintf("%020d.log", offset))
		s.Index = filepath.Join(dir, fmt.Sprintf("%020d.index", offset))
		s.DataFormat = message.FormatLog
		s.IndexFormat = index.FormatLog
	}
	return s
}

type Stats struct {
	Segments int
	Messages int
	Size     int64
}

func (s Segment) Stat(params index.Params) (Stats, error) {
	dataStat, err := os.Stat(s.Data)
	if err != nil {
		return Stats{}, fmt.Errorf("stat log: %w", err)
	}

	indexStat, err := os.Stat(s.Index)
	if err != nil {
		return Stats{}, fmt.Errorf("stat index: %w", err)
	}

	indexDataSize := indexStat.Size()
	if s.IndexFormat == index.FormatSegment {
		indexDataSize -= index.HeaderSize
	}
	if indexDataSize < 0 {
		// index is smaller than its header — treat as empty (NeedsReindex handles repair)
		indexDataSize = 0
	}

	return Stats{
		Segments: 1,
		Messages: int(indexDataSize / params.Size()),
		Size:     dataStat.Size() + indexStat.Size(),
	}, nil
}

var errIndexSize = fmt.Errorf("%w: incorrect size", index.ErrCorrupted)
var errIndexItem = fmt.Errorf("%w: incorrect item", index.ErrCorrupted)

func (s Segment) Check(params index.Params) error {
	log, err := message.OpenReader(s.Data, s.DataFormat)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	var position = log.InitialPosition()
	var indexTime int64
	var logIndex []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		item := params.NewItem(msg, position, indexTime)
		logIndex = append(logIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	switch items, err := index.Read(s.Index, params, s.IndexFormat); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(logIndex) != len(items):
		return errIndexSize
	default:
		for i, item := range logIndex {
			if item != items[i] {
				return errIndexItem
			}
		}
	}

	return nil
}

func (s Segment) Recover(params index.Params) error {
	log, err := message.OpenReader(s.Data, s.DataFormat)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	restore, err := message.OpenWriter(s.Data+".recover", s.DataFormat)
	if err != nil {
		return err
	}
	defer func() { _ = restore.Close() }() // ignoring since its only applicable if an error has happened

	var position = log.InitialPosition()
	var indexTime int64
	var corrupted = false
	var logIndex []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if errors.Is(err, message.ErrCorrupted) {
			corrupted = true
			break
		} else if err != nil {
			return err
		}

		if _, err := restore.Write(msg); err != nil {
			return err
		}

		item := params.NewItem(msg, position, indexTime)
		logIndex = append(logIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	if err := log.Close(); err != nil {
		return err
	}
	if err := restore.SyncAndClose(); err != nil {
		return err
	}

	if corrupted {
		if err := os.Rename(restore.Path, log.Path); err != nil {
			return fmt.Errorf("restore log rename: %w", err)
		}
	} else {
		if err := os.Remove(restore.Path); err != nil {
			return fmt.Errorf("restore log delete: %w", err)
		}
	}

	var corruptedIndex = false
	switch items, err := index.Read(s.Index, params, s.IndexFormat); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case errors.Is(err, index.ErrCorrupted):
		corruptedIndex = true
	case err != nil:
		return err
	case len(logIndex) != len(items):
		corruptedIndex = true
	default:
		for i, item := range logIndex {
			if item != items[i] {
				corruptedIndex = true
				break
			}
		}
	}

	if corruptedIndex {
		if err := os.Remove(s.Index); err != nil {
			return fmt.Errorf("restore index delete: %w", err)
		}
	}

	if err := s.syncDir(); err != nil {
		return fmt.Errorf("restore sync dir: %w", err)
	}

	return nil
}

func (s Segment) NeedsReindex() (bool, error) {
	switch info, err := os.Stat(s.Index); {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("needs reindex stat: %w", err)
	case info.Size() == 0:
		return true, nil
	case s.IndexFormat == index.FormatSegment && info.Size() <= index.HeaderSize:
		// header-only or partial header: no index entries present
		return true, nil
	default:
		return false, nil
	}
}

func (s Segment) ReindexAndReadIndex(params index.Params) ([]index.Item, error) {
	switch reindex, err := s.NeedsReindex(); {
	case err != nil:
		return nil, err
	case reindex:
		return s.Reindex(params)
	default:
		return index.Read(s.Index, params, s.IndexFormat)
	}
}

func (s Segment) Reindex(params index.Params) ([]index.Item, error) {
	log, err := message.OpenReader(s.Data, s.DataFormat)
	if err != nil {
		return nil, err
	}
	defer func() { _ = log.Close() }()

	return s.ReindexReader(params, log)
}

func (s Segment) ReindexReader(params index.Params, log *message.Reader) ([]index.Item, error) {
	var position = log.InitialPosition()
	var indexTime int64
	var items []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		item := params.NewItem(msg, position, indexTime)
		items = append(items, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	if err := index.Write(s.Index, params, items, s.IndexFormat); err != nil {
		return nil, err
	}
	return items, nil
}

func (s Segment) Backup(targetDir string) error {
	logName, err := filepath.Rel(s.Dir, s.Data)
	if err != nil {
		return fmt.Errorf("backup log rel: %w", err)
	}
	targetLog := filepath.Join(targetDir, logName)
	if err := copyFile(s.Data, targetLog); err != nil {
		return fmt.Errorf("backup log copy: %w", err)
	}

	indexName, err := filepath.Rel(s.Dir, s.Index)
	if err != nil {
		return fmt.Errorf("backup index rel: %w", err)
	}
	targetIndex := filepath.Join(targetDir, indexName)
	if err := copyFile(s.Index, targetIndex); err != nil {
		return fmt.Errorf("backup index copy: %w", err)
	}

	return nil
}

func (olds Segment) Migrate() (Segment, error) {
	if olds.DataFormat == message.FormatSegment {
		return olds, nil
	}

	// Remove the index first to guarantee its rebuild and migrated
	switch err := os.Remove(olds.Index); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return Segment{}, fmt.Errorf("migrate index remove: %w", err)
	}

	s := New(olds.Dir, olds.Offset, message.FormatSegment)

	oldLog, err := message.OpenReader(olds.Data, olds.DataFormat)
	if err != nil {
		return Segment{}, fmt.Errorf("migrate open reader: %w", err)
	}
	defer func() { _ = oldLog.Close() }()

	migratedPath := s.Data + ".migrate"
	if err := os.Remove(migratedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Segment{}, fmt.Errorf("migrate remove stale temp: %w", err)
	}
	migratedLog, err := message.OpenWriter(migratedPath, s.DataFormat)
	if err != nil {
		return Segment{}, fmt.Errorf("migrate open writer: %w", err)
	}
	defer func() { _ = migratedLog.Close() }() // ignoring since its only applicable if an error has happened

	var position = oldLog.InitialPosition()
	for {
		msg, nextPosition, err := oldLog.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return Segment{}, fmt.Errorf("migrate read: %w", err)
		}

		if _, err := migratedLog.Write(msg); err != nil {
			return Segment{}, fmt.Errorf("migrate write: %w", err)
		}

		position = nextPosition
	}

	if err := oldLog.Close(); err != nil {
		return Segment{}, fmt.Errorf("migrate close old log: %w", err)
	}
	if err := migratedLog.SyncAndClose(); err != nil {
		return Segment{}, fmt.Errorf("migrate close migrated log: %w", err)
	}

	if err := os.Rename(migratedLog.Path, s.Data); err != nil {
		return Segment{}, fmt.Errorf("migrate log rename: %w", err)
	}
	if err := os.Remove(oldLog.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return Segment{}, fmt.Errorf("migrate old log delete: %w", err)
	}

	if err := s.syncDir(); err != nil {
		return Segment{}, fmt.Errorf("migrate sync dir: %w", err)
	}

	return s, nil
}

func (olds Segment) Rename(news Segment) error {
	if err := os.Rename(olds.Data, news.Data); err != nil {
		return fmt.Errorf("rename log rename: %w", err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("rename index rename: %w", err)
	}

	if err := news.syncDir(); err != nil {
		return fmt.Errorf("rename sync dir: %w", err)
	}

	return nil
}

func (olds Segment) Override(news Segment) error {
	// remove index segment so we don't have invalid index
	if err := os.Remove(news.Index); err != nil {
		return fmt.Errorf("override index delete: %w", err)
	}

	if err := os.Rename(olds.Data, news.Data); err != nil {
		return fmt.Errorf("override log rename: %w", err)
	}
	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("override index rename: %w", err)
	}

	if err := news.syncDir(); err != nil {
		return fmt.Errorf("override sync dir: %w", err)
	}

	return nil
}

func (s Segment) Remove() error {
	if err := os.Remove(s.Index); err != nil {
		return fmt.Errorf("remove index delete: %w", err)
	}
	if err := os.Remove(s.Data); err != nil {
		return fmt.Errorf("remove log delete: %w", err)
	}

	if err := s.syncDir(); err != nil {
		return fmt.Errorf("remove sync dir: %w", err)
	}

	return nil
}

func (s Segment) RemoveData() error {
	if err := os.Remove(s.Data); err != nil {
		return fmt.Errorf("remove log delete: %w", err)
	}
	if err := s.syncDir(); err != nil {
		return fmt.Errorf("remove sync dir: %w", err)
	}
	return nil
}

func (s Segment) syncDir() error {
	return kdir.Sync(s.Dir)
}

type RewriteSegment struct {
	Segment
	Stats Stats

	SurviveOffsets map[int64]struct{}
	DeletedOffsets map[int64]struct{}
	DeletedSize    int64
}

func (r *RewriteSegment) GetNewSegment() Segment {
	lowestOffset := message.MinOffset(r.SurviveOffsets)
	return New(r.Dir, lowestOffset, r.DataFormat)
}

func (s Segment) forRewrite() (*RewriteSegment, error) {
	randStr, err := randStr(5)
	if err != nil {
		return nil, err
	}

	dst := &RewriteSegment{
		Segment: New(s.Dir, s.Offset, message.FormatSegment),

		SurviveOffsets: map[int64]struct{}{},
		DeletedOffsets: map[int64]struct{}{},
	}

	dst.Data = fmt.Sprintf("%s.rewrite.%s", s.Data, randStr)
	dst.Index = fmt.Sprintf("%s.rewrite.%s", s.Index, randStr)

	return dst, nil
}

func (src Segment) Rewrite(dropOffsets map[int64]struct{}, params index.Params) (*RewriteSegment, error) {
	dst, err := src.forRewrite()
	if err != nil {
		return nil, err
	}

	srcLog, err := message.OpenReader(src.Data, src.DataFormat)
	if err != nil {
		return nil, err
	}
	defer func() { _ = srcLog.Close() }()

	dstLog, err := message.OpenWriter(dst.Data, dst.DataFormat)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dstLog.Close() }() // ignoring since its only applicable if an error has happened

	var srcPosition = srcLog.InitialPosition()
	var indexTime int64
	var dstItems []index.Item
	for {
		msg, nextSrcPosition, err := srcLog.Read(srcPosition)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, err
		}

		if _, ok := dropOffsets[msg.Offset]; ok {
			dst.DeletedOffsets[msg.Offset] = struct{}{}
			dst.DeletedSize += message.Size(msg, src.DataFormat) + params.Size()
		} else {
			dstPosition, err := dstLog.Write(msg)
			if err != nil {
				return nil, err
			}
			dst.SurviveOffsets[msg.Offset] = struct{}{}

			item := params.NewItem(msg, dstPosition, indexTime)
			dstItems = append(dstItems, item)
			indexTime = item.Timestamp
		}

		srcPosition = nextSrcPosition
	}

	if err := srcLog.Close(); err != nil {
		return nil, err
	}
	if err := dstLog.SyncAndClose(); err != nil {
		return nil, err
	}
	if err := index.Write(dst.Index, params, dstItems, dst.IndexFormat); err != nil {
		return nil, err
	}

	dst.Stats, err = dst.Stat(params)
	if err != nil {
		return nil, err
	}
	return dst, nil
}
