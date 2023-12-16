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
	"github.com/klev-dev/kleverr"
)

type Segment[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	Dir    string
	Offset int64

	Log   string
	Index string
}

type Stats struct {
	Segments int
	Messages int
	Size     int64
}

func New[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir string, offset int64) Segment[IX, IT, IC, IS] {
	return Segment[IX, IT, IC, IS]{
		Dir:    dir,
		Offset: offset,

		Log:   filepath.Join(dir, fmt.Sprintf("%020d.log", offset)),
		Index: filepath.Join(dir, fmt.Sprintf("%020d.index", offset)),
	}
}

func (s Segment[IX, IT, IC, IS]) GetOffset() int64 {
	return s.Offset
}

func (s Segment[IX, IT, IC, IS]) Stat(ix IX) (Stats, error) {
	logStat, err := os.Stat(s.Log)
	if err != nil {
		return Stats{}, kleverr.Newf("could not stat log: %w", err)
	}

	indexStat, err := os.Stat(s.Index)
	if err != nil {
		return Stats{}, kleverr.Newf("could not stat index: %w", err)
	}

	return Stats{
		Segments: 1,
		Messages: int(indexStat.Size() / ix.Size()),
		Size:     logStat.Size() + indexStat.Size(),
	}, nil
}

func (s Segment[IX, IT, IC, IS]) Check(ix IX) error {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return err
	}
	defer log.Close()

	var position int64
	var indexCtx = ix.NewContext()
	var logIndex []IT
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return kleverr.Newf("%s: %w", s.Log, err)
		}

		item, nextContext, err := ix.New(msg, position, indexCtx)
		if err != nil {
			return err
		}
		logIndex = append(logIndex, item)

		position = nextPosition
		indexCtx = nextContext
	}

	switch items, err := index.Read(s.Index, ix); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(logIndex) != len(items):
		return kleverr.Newf("%s: incorrect index size: %w", s.Index, index.ErrCorrupted)
	default:
		for i, item := range logIndex {
			if !item.Equal(items[i]) {
				return kleverr.Newf("%s: incorrect index item: %w", s.Index, index.ErrCorrupted)
			}
		}
	}

	return nil
}

func (s Segment[IX, IT, IC, IS]) Recover(ix IX) error {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return err
	}
	defer log.Close()

	restore, err := message.OpenWriter(s.Log + ".recover")
	if err != nil {
		return err
	}
	defer restore.Close()

	var position int64
	var corrupted = false
	var indexCtx = ix.NewContext()
	var logIndex []IT
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

		item, nextContext, err := ix.New(msg, position, indexCtx)
		if err != nil {
			return err
		}
		logIndex = append(logIndex, item)

		position = nextPosition
		indexCtx = nextContext
	}

	if err := log.Close(); err != nil {
		return err
	}
	if err := restore.Sync(); err != nil {
		return err
	}
	if err := restore.Close(); err != nil {
		return err
	}

	if corrupted {
		if err := os.Rename(restore.Path, log.Path); err != nil {
			return kleverr.Newf("could not rename restore: %w", err)
		}
	} else {
		if err := os.Remove(restore.Path); err != nil {
			return kleverr.Newf("could not delete restore: %w", err)
		}
	}

	var corruptedIndex = false
	switch items, err := index.Read(s.Index, ix); {
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
			if !item.Equal(items[i]) {
				corruptedIndex = true
				break
			}
		}
	}

	if corruptedIndex {
		if err := os.Remove(s.Index); err != nil {
			return kleverr.Newf("could not remove corrupted index: %w", err)
		}
	}

	return nil
}

func (s Segment[IX, IT, IC, IS]) NeedsReindex() (bool, error) {
	switch info, err := os.Stat(s.Index); {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, kleverr.Newf("could not stat index: %w", err)
	case info.Size() == 0:
		return true, nil
	default:
		return false, nil
	}
}

func (s Segment[IX, IT, IC, IS]) ReindexAndReadIndex(ix IX) ([]IT, error) {
	switch reindex, err := s.NeedsReindex(); {
	case err != nil:
		return nil, err
	case reindex:
		return s.Reindex(ix)
	default:
		return index.Read(s.Index, ix)
	}
}

func (s Segment[IX, IT, IC, IS]) Reindex(ix IX) ([]IT, error) {
	log, err := message.OpenReader(s.Log)
	if err != nil {
		return nil, err
	}
	defer log.Close()

	return s.ReindexReader(ix, log)
}

func (s Segment[IX, IT, IC, IS]) ReindexReader(ix IX, log *message.Reader) ([]IT, error) {
	var position int64
	var indexCtx = ix.NewContext()
	var items []IT
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		item, nextContext, err := ix.New(msg, position, indexCtx)
		if err != nil {
			return nil, err
		}
		items = append(items, item)

		position = nextPosition
		indexCtx = nextContext
	}

	if err := index.Write[IX, IT](s.Index, ix, items); err != nil {
		return nil, err
	}
	return items, nil
}

func (s Segment[IX, IT, IC, IS]) Backup(targetDir string) error {
	logName, err := filepath.Rel(s.Dir, s.Log)
	if err != nil {
		return kleverr.Newf("could not rel log: %w", err)
	}
	targetLog := filepath.Join(targetDir, logName)
	if err := copyFile(s.Log, targetLog); err != nil {
		return kleverr.Newf("could not copy log: %w", err)
	}

	indexName, err := filepath.Rel(s.Dir, s.Index)
	if err != nil {
		return kleverr.Newf("could not rel index: %w", err)
	}
	targetIndex := filepath.Join(targetDir, indexName)
	if err := copyFile(s.Index, targetIndex); err != nil {
		return kleverr.Newf("could not copy index: %w", err)
	}

	return nil
}

func (s Segment[IX, IT, IC, IS]) ForRewrite() (Segment[IX, IT, IC, IS], error) {
	randStr, err := randStr(8)
	if err != nil {
		return Segment[IX, IT, IC, IS]{}, nil
	}

	s.Log = fmt.Sprintf("%s.rewrite.%s", s.Log, randStr)
	s.Index = fmt.Sprintf("%s.rewrite.%s", s.Index, randStr)
	return s, nil
}

func (olds Segment[IX, IT, IC, IS]) Rename(news Segment[IX, IT, IC, IS]) error {
	if err := os.Rename(olds.Log, news.Log); err != nil {
		return kleverr.Newf("could not rename log: %w", err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return kleverr.Newf("could not rename index: %w", err)
	}

	return nil
}

func (olds Segment[IX, IT, IC, IS]) Override(news Segment[IX, IT, IC, IS]) error {
	// remove index segment so we don't have invalid index
	if err := os.Remove(news.Index); err != nil {
		return kleverr.Newf("could not delete index: %w", err)
	}

	if err := os.Rename(olds.Log, news.Log); err != nil {
		return kleverr.Newf("could not rename log: %w", err)
	}

	if err := os.Rename(olds.Index, news.Index); err != nil {
		return kleverr.Newf("could not rename index: %w", err)
	}

	return nil
}

func (s Segment[IX, IT, IC, IS]) Remove() error {
	if err := os.Remove(s.Index); err != nil {
		return kleverr.Newf("could not delete index: %w", err)
	}
	if err := os.Remove(s.Log); err != nil {
		return kleverr.Newf("could not delete log: %w", err)
	}
	return nil
}

type RewriteSegment[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore] struct {
	Segment Segment[IX, IT, IC, IS]
	Stats   Stats

	SurviveOffsets map[int64]struct{}
	DeletedOffsets map[int64]struct{}
	DeletedSize    int64
}

func (r *RewriteSegment[IX, IT, IC, IS]) GetNewSegment() Segment[IX, IT, IC, IS] {
	orderedOffsets := maps.Keys(r.SurviveOffsets)
	slices.Sort(orderedOffsets)
	lowestOffset := orderedOffsets[0]
	return New[IX, IT](r.Segment.Dir, lowestOffset)
}

func (src Segment[IX, IT, IC, IS]) Rewrite(dropOffsets map[int64]struct{}, ix IX) (*RewriteSegment[IX, IT, IC, IS], error) {
	dst, err := src.ForRewrite()
	if err != nil {
		return nil, err
	}

	result := &RewriteSegment[IX, IT, IC, IS]{Segment: dst}

	srcLog, err := message.OpenReader(src.Log)
	if err != nil {
		return nil, err
	}
	defer srcLog.Close()

	dstLog, err := message.OpenWriter(dst.Log)
	if err != nil {
		return nil, err
	}
	defer dstLog.Close()

	result.SurviveOffsets = map[int64]struct{}{}
	result.DeletedOffsets = map[int64]struct{}{}

	var srcPosition int64
	var indexCtx = ix.NewContext()
	var dstItems []IT
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
			result.DeletedSize += message.Size(msg) + ix.Size()
		} else {
			dstPosition, err := dstLog.Write(msg)
			if err != nil {
				return nil, err
			}
			result.SurviveOffsets[msg.Offset] = struct{}{}

			item, nextContext, err := ix.New(msg, dstPosition, indexCtx)
			if err != nil {
				return nil, err
			}
			dstItems = append(dstItems, item)
			indexCtx = nextContext
		}

		srcPosition = nextSrcPosition
	}

	if err := srcLog.Close(); err != nil {
		return nil, err
	}
	if err := dstLog.SyncAndClose(); err != nil {
		return nil, err
	}
	if err := index.Write(dst.Index, ix, dstItems); err != nil {
		return nil, err
	}

	result.Stats, err = dst.Stat(ix)
	if err != nil {
		return nil, err
	}
	return result, nil
}
