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
		return Stats{}, fmt.Errorf("stat log: %w", err)
	}

	indexStat, err := os.Stat(s.Index)
	if err != nil {
		return Stats{}, fmt.Errorf("stat index: %w", err)
	}

	return Stats{
		Segments: 1,
		Messages: int(indexStat.Size() / params.Size()),
		Size:     logStat.Size() + indexStat.Size(),
	}, nil
}

var errIndexSize = fmt.Errorf("%w: incorrect size", index.ErrCorrupted)
var errIndexItem = fmt.Errorf("%w: incorrect item", index.ErrCorrupted)

func (s Segment) Check(params index.Params) error {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	var position, indexTime int64
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

	switch items, err := index.Read(s.Index, params); {
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
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	restore, err := message.OpenWriter(s.Log + ".recover")
	if err != nil {
		return err
	}
	defer func() { _ = restore.Close() }() // ignoring since its only applicable if an error has happened

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
	switch items, err := index.Read(s.Index, params); {
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
		return index.Read(s.Index, params)
	}
}

func (s Segment) Reindex(params index.Params) ([]index.Item, error) {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return nil, err
	}
	defer func() { _ = log.Close() }()

	return s.ReindexReader(params, log)
}

func (s Segment) ReindexReader(params index.Params, log *message.Reader) ([]index.Item, error) {
	var position, indexTime int64
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

	if err := index.Write(s.Index, params, items); err != nil {
		return nil, err
	}
	return items, nil
}

func (s Segment) Backup(targetDir string) error {
	logName, err := filepath.Rel(s.Dir, s.Log)
	if err != nil {
		return fmt.Errorf("backup log rel: %w", err)
	}
	targetLog := filepath.Join(targetDir, logName)
	if err := copyFile(s.Log, targetLog); err != nil {
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

func (s Segment) ForRewrite() (Segment, error) {
	randStr, err := randStr(5)
	if err != nil {
		return Segment{}, err
	}

	s.Log = fmt.Sprintf("%s.rewrite.%s", s.Log, randStr)
	s.Index = fmt.Sprintf("%s.rewrite.%s", s.Index, randStr)
	return s, nil
}

func (olds Segment) Rename(news Segment) error {
	if err := os.Rename(olds.Log, news.Log); err != nil {
		return fmt.Errorf("rename log rename: %w", err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("rename index rename: %w", err)
	}

	return nil
}

func (olds Segment) Override(news Segment) error {
	// remove index segment so we don't have invalid index
	if err := os.Remove(news.Index); err != nil {
		return fmt.Errorf("override index delete: %w", err)
	}

	if err := os.Rename(olds.Log, news.Log); err != nil {
		return fmt.Errorf("override log rename: %w", err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return fmt.Errorf("override index rename: %w", err)
	}

	return nil
}

func (s Segment) Remove() error {
	if err := os.Remove(s.Index); err != nil {
		return fmt.Errorf("remove index delete: %w", err)
	}
	if err := os.Remove(s.Log); err != nil {
		return fmt.Errorf("remove log delete: %w", err)
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
		return nil, err
	}

	result := &RewriteSegment{Segment: dst}

	srcLog, err := message.OpenReader(src.Log)
	if err != nil {
		return nil, err
	}
	defer func() { _ = srcLog.Close() }()

	dstLog, err := message.OpenWriter(dst.Log)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dstLog.Close() }() // ignoring since its only applicable if an error has happened

	result.SurviveOffsets = map[int64]struct{}{}
	result.DeletedOffsets = map[int64]struct{}{}

	var srcPosition, indexTime int64
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
			result.DeletedOffsets[msg.Offset] = struct{}{}
			result.DeletedSize += message.Size(msg) + params.Size()
		} else {
			dstPosition, err := dstLog.Write(msg)
			if err != nil {
				return nil, err
			}
			result.SurviveOffsets[msg.Offset] = struct{}{}

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
	if err := index.Write(dst.Index, params, dstItems); err != nil {
		return nil, err
	}

	result.Stats, err = dst.Stat(params)
	if err != nil {
		return nil, err
	}
	return result, nil
}
