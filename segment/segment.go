package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
)

type Segment struct {
	Dir    string
	Offset int64

	Log   string
	Index string
}

func (s Segment) GetOffset() int64 {
	return s.Offset
}

func New(dir string, offset int64) Segment {
	return Segment{
		Dir:    dir,
		Offset: offset,

		Log:   filepath.Join(dir, fmt.Sprintf("%020d.log", offset)),
		Index: filepath.Join(dir, fmt.Sprintf("%020d.index", offset)),
	}
}

type Stats struct {
	Segments int
	Messages int
	Size     int64
}

func (s Segment) Stat(params index.Params) (Stats, error) {
	logStat, err := os.Stat(s.Log)
	if err != nil {
		return Stats{}, fmt.Errorf("[segment.Segment.Stat] %s stat: %w", s.Log, err)
	}

	indexStat, err := os.Stat(s.Index)
	if err != nil {
		return Stats{}, fmt.Errorf("[segment.Segment.Stat] %s stat: %w", s.Index, err)
	}

	return Stats{
		Segments: 1,
		Messages: int(indexStat.Size() / params.Size()),
		Size:     logStat.Size() + indexStat.Size(),
	}, nil
}

func (s Segment) Check(params index.Params) error {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return fmt.Errorf("[segment.Segment.Check] %s open reader: %w", s.Log, err)
	}
	defer log.Close()

	var position, indexTime int64
	var logIndex []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("[segment.Segment.Check] %s read: %w", s.Log, err)
		}

		item := params.NewItem(msg, position, indexTime)
		logIndex = append(logIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	switch items, err := index.Read(s.Index, params); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("[segment.Segment.Check] %s read: %w", s.Index, err)
	case len(logIndex) != len(items):
		return fmt.Errorf("[segment.Segment.Check] %s incorrect index size: %w", s.Index, index.ErrCorrupted)
	default:
		for i, item := range logIndex {
			if item != items[i] {
				return fmt.Errorf("[segment.Segment.Check] %s incorrect index item %d: %w", s.Index, i, index.ErrCorrupted)
			}
		}
	}

	return nil
}

func (s Segment) Recover(params index.Params) error {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return fmt.Errorf("[segment.Segment.Recover] %s open reader: %w", s.Log, err)
	}
	defer log.Close()

	restore, err := message.OpenWriter(s.Log + ".recover")
	if err != nil {
		return fmt.Errorf("[segment.Segment.Recover] %s.recover open writer: %w", s.Log, err)
	}
	defer restore.Close()

	var position, indexTime int64
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
			return fmt.Errorf("[segment.Segment.Recover] %s read %d: %w", s.Log, position, err)
		}

		if _, err := restore.Write(msg); err != nil {
			return fmt.Errorf("[segment.Segment.Recover] %s.recover write %d: %w", s.Log, position, err)
		}

		item := params.NewItem(msg, position, indexTime)
		logIndex = append(logIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	if err := log.Close(); err != nil {
		return fmt.Errorf("[segment.Segment.Recover] %s close: %w", s.Log, err)
	}
	if err := restore.SyncAndClose(); err != nil {
		return fmt.Errorf("[segment.Segment.Recover] %s.recover sync close: %w", s.Log, err)
	}

	if corrupted {
		if err := os.Rename(restore.Path, log.Path); err != nil {
			return fmt.Errorf("[segment.Segment.Recover] %s.recover rename to %s: %w", s.Log, s.Log, err)
		}
	} else {
		if err := os.Remove(restore.Path); err != nil {
			return fmt.Errorf("[segment.Segment.Recover] %s.recover remove: %w", s.Log, err)
		}
	}

	var corruptedIndex = false
	switch items, err := index.Read(s.Index, params); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case errors.Is(err, index.ErrCorrupted):
		corruptedIndex = true
	case err != nil:
		return fmt.Errorf("[segment.Segment.Recover] %s read: %w", s.Index, err)
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
			return fmt.Errorf("[segment.Segment.Recover] %s remove: %w", s.Index, err)
		}
	}

	return nil
}

func (s Segment) NeedsReindex() (bool, error) {
	switch info, err := os.Stat(s.Index); {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("[segment.Segment.NeedsReindex] %s stat: %w", s.Index, err)
	case info.Size() == 0:
		return true, nil
	default:
		return false, nil
	}
}

func (s Segment) ReindexAndReadIndex(params index.Params) ([]index.Item, error) {
	switch reindex, err := s.NeedsReindex(); {
	case err != nil:
		return nil, fmt.Errorf("[segment.Segment.ReindexAndReadIndex] %s needs reindex: %w", s.Index, err)
	case reindex:
		items, err := s.Reindex(params)
		if err != nil {
			return nil, fmt.Errorf("[segment.Segment.ReindexAndReadIndex] %s reindex: %w", s.Index, err)
		}
		return items, nil
	default:
		items, err := index.Read(s.Index, params)
		if err != nil {
			return nil, fmt.Errorf("[segment.Segment.ReindexAndReadIndex] %s read: %w", s.Index, err)
		}
		return items, nil
	}
}

func (s Segment) Reindex(params index.Params) ([]index.Item, error) {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Reindex] %s open reader: %w", s.Log, err)
	}
	defer log.Close()

	items, err := s.ReindexReader(params, log)
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Reindex] %s reindex reader: %w", s.Index, err)
	}
	return items, nil
}

func (s Segment) ReindexReader(params index.Params, log *message.Reader) ([]index.Item, error) {
	var position, indexTime int64
	var items []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, fmt.Errorf("[segment.Segment.ReindexReader] %s read: %w", s.Log, err)
		}

		item := params.NewItem(msg, position, indexTime)
		items = append(items, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	if err := index.Write(s.Index, params, items); err != nil {
		return nil, fmt.Errorf("[segment.Segment.ReindexReader] %s write: %w", s.Index, err)
	}
	return items, nil
}

func (s Segment) Backup(targetDir string) error {
	logName, err := filepath.Rel(s.Dir, s.Log)
	if err != nil {
		return fmt.Errorf("[segment.Segment.Backup] %s log rel: %w", s.Log, err)
	}
	targetLog := filepath.Join(targetDir, logName)
	if err := copyFile(s.Log, targetLog); err != nil {
		return fmt.Errorf("[segment.Segment.Backup] %s copy log to %s: %w", s.Log, targetLog, err)
	}

	indexName, err := filepath.Rel(s.Dir, s.Index)
	if err != nil {
		return fmt.Errorf("[segment.Segment.Backup] %s index rel: %w", s.Index, err)
	}
	targetIndex := filepath.Join(targetDir, indexName)
	if err := copyFile(s.Index, targetIndex); err != nil {
		return fmt.Errorf("[segment.Segment.Backup] %s copy index to %s: %w", s.Index, targetIndex, err)
	}

	return nil
}

func (s Segment) ForRewrite() (Segment, error) {
	randStr, err := randStr(8)
	if err != nil {
		return Segment{}, fmt.Errorf("[segment.Segment.ForRewrite] %d rand: %w", s.Offset, err)
	}

	s.Log = fmt.Sprintf("%s.rewrite.%s", s.Log, randStr)
	s.Index = fmt.Sprintf("%s.rewrite.%s", s.Index, randStr)
	return s, nil
}

func (olds Segment) Rename(news Segment) error {
	if err := os.Rename(olds.Log, news.Log); err != nil {
		return fmt.Errorf("[segment.Segment.Rename] %s log rename to %s: %w", olds.Log, news.Log, err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("[segment.Segment.Rename] %s index rename to %s: %w", olds.Index, news.Index, err)
	}

	return nil
}

func (olds Segment) Override(news Segment) error {
	// remove index segment so we don't have invalid index
	if err := os.Remove(news.Index); err != nil {
		return fmt.Errorf("[segment.Segment.Override] %s index remove: %w", news.Index, err)
	}

	if err := os.Rename(olds.Log, news.Log); err != nil {
		return fmt.Errorf("[segment.Segment.Override] %s log rename to %s: %w", olds.Log, news.Log, err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("[segment.Segment.Override] %s index rename to %s: %w", olds.Index, news.Index, err)
	}

	return nil
}

func (s Segment) Remove() error {
	if err := os.Remove(s.Index); err != nil {
		return fmt.Errorf("[segment.Segment.Remove] %s index remove: %w", s.Index, err)
	}
	if err := os.Remove(s.Log); err != nil {
		return fmt.Errorf("[segment.Segment.Remove] %s log remove: %w", s.Log, err)
	}
	return nil
}

type RewriteSegment struct {
	Segment Segment
	Stats   Stats

	SurviveOffsets map[int64]struct{}
	DeletedOffsets map[int64]struct{}
	DeletedSize    int64
}

func (r *RewriteSegment) GetNewSegment() Segment {
	orderedOffsets := maps.Keys(r.SurviveOffsets)
	slices.Sort(orderedOffsets)
	lowestOffset := orderedOffsets[0]
	return New(r.Segment.Dir, lowestOffset)
}

func (src Segment) Rewrite(dropOffsets map[int64]struct{}, params index.Params) (*RewriteSegment, error) {
	dst, err := src.ForRewrite()
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %d for rewrite: %w", src.Offset, err)
	}

	result := &RewriteSegment{Segment: dst}

	srcLog, err := message.OpenReader(src.Log)
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s open reader: %w", src.Log, err)
	}
	defer srcLog.Close()

	dstLog, err := message.OpenWriter(dst.Log)
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s open writer: %w", dst.Log, err)
	}
	defer dstLog.Close()

	result.SurviveOffsets = map[int64]struct{}{}
	result.DeletedOffsets = map[int64]struct{}{}

	var srcPosition, indexTime int64
	var dstItems []index.Item
LOOP:
	for {
		msg, nextSrcPosition, err := srcLog.Read(srcPosition)
		switch {
		case err == nil:
		case errors.Is(err, io.EOF):
			break LOOP
		default:
			return nil, fmt.Errorf("[segment.Segment.Rewrite] %s read %d: %w", src.Log, srcPosition, err)
		}

		if _, ok := dropOffsets[msg.Offset]; ok {
			result.DeletedOffsets[msg.Offset] = struct{}{}
			result.DeletedSize += message.Size(msg) + params.Size()
		} else {
			dstPosition, err := dstLog.Write(msg)
			if err != nil {
				return nil, fmt.Errorf("[segment.Segment.Rewrite] %s write: %w", dst.Log, err)
			}
			result.SurviveOffsets[msg.Offset] = struct{}{}

			item := params.NewItem(msg, dstPosition, indexTime)
			dstItems = append(dstItems, item)
			indexTime = item.Timestamp
		}

		srcPosition = nextSrcPosition
	}

	if err := srcLog.Close(); err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s close: %w", src.Log, err)
	}
	if err := dstLog.SyncAndClose(); err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s sync close: %w", dst.Log, err)
	}
	if err := index.Write(dst.Index, params, dstItems); err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s write: %w", dst.Index, err)
	}

	result.Stats, err = dst.Stat(params)
	if err != nil {
		return nil, fmt.Errorf("[segment.Segment.Rewrite] %s stat: %w", dst.Log, err)
	}
	return result, nil
}
