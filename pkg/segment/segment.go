package segment

import (
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"

	"github.com/klev-dev/klevdb/pkg/index"
	"github.com/klev-dev/klevdb/pkg/kdir"
	"github.com/klev-dev/klevdb/pkg/message"
)

type Segment struct {
	Dir    string
	Offset int64

	Log   string
	Index string

	AutoSync bool
}

func (s Segment) GetOffset() int64 {
	return s.Offset
}

func New(dir string, offset int64, autoSync bool) Segment {
	return Segment{
		Dir:    dir,
		Offset: offset,

		Log:   filepath.Join(dir, fmt.Sprintf("%020d.log", offset)),
		Index: filepath.Join(dir, fmt.Sprintf("%020d.index", offset)),

		AutoSync: autoSync,
	}
}

func (s Segment) NewAt(offset int64) Segment {
	return New(s.Dir, offset, s.AutoSync)
}

type Stats struct {
	Segments int
	Messages int
	Size     int64
}

func (s Segment) Stat(params index.Params) (Stats, error) {
	dataStat, err := os.Stat(s.Log)
	if err != nil {
		return Stats{}, fmt.Errorf("stat log: %w", err)
	}

	indexSize, indexMessages, err := index.Stat(s.Index, s.Offset, params)
	if err != nil {
		return Stats{}, fmt.Errorf("stat index: %w", err)
	}

	return Stats{
		Segments: 1,
		Messages: indexMessages,
		Size:     dataStat.Size() + indexSize,
	}, nil
}

func (s Segment) Check(params index.Params) error {
	log, err := message.OpenReader(s.Log, s.Offset)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	var position = log.InitialPosition()
	var indexTime int64
	var checkIndex []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return err
		}

		item := params.NewItem(msg, position, indexTime)
		checkIndex = append(checkIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	switch items, err := index.Read(s.Index, s.Offset, params); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case !slices.Equal(checkIndex, items):
		return index.ErrCorrupted
	}

	return nil
}

func (s Segment) Recover(params index.Params) error {
	log, err := message.OpenReader(s.Log, s.Offset)
	if err != nil {
		return err
	}
	defer func() { _ = log.Close() }()

	restore, err := message.OpenWriter(s.Log+".recover", s.Offset, log.Version())
	if err != nil {
		return err
	}
	defer func() { _ = restore.Close() }() // ignoring since its only applicable if an error has happened

	var position = log.InitialPosition()
	var indexTime int64
	var corrupted = false
	var restoreIndex []index.Item
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
		restoreIndex = append(restoreIndex, item)
		indexTime = item.Timestamp

		position = nextPosition
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
	var indexVersion = index.VUnknown
	switch items, err := index.Read(s.Index, s.Offset, params); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case errors.Is(err, index.ErrCorrupted):
		corruptedIndex = true
	case err != nil:
		return err
	case !slices.Equal(items, restoreIndex):
		indexVersion, _ = index.GetVersion(s.Index, s.Offset, params)
		corruptedIndex = true
	}

	if corruptedIndex {
		if err := os.Remove(s.Index); err != nil {
			return fmt.Errorf("restore index delete: %w", err)
		}
		if indexVersion != index.VUnknown {
			if err := index.Write(s.Index, s.Offset, indexVersion, params, restoreIndex); err != nil {
				return fmt.Errorf("restore index write: %w", err)
			}
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
	case info.Size() <= index.HeaderSize:
		// V2: header-only (no entries); V1: empty or partial first entry — reindex either way
		return true, nil
	default:
		return false, nil
	}
}

func (s Segment) ReindexAndReadIndex(params index.Params, version index.Version) ([]index.Item, error) {
	switch reindex, err := s.NeedsReindex(); {
	case err != nil:
		return nil, err
	case reindex:
		return s.Reindex(params, version)
	default:
		return index.Read(s.Index, s.Offset, params)
	}
}

func (s Segment) Reindex(params index.Params, version index.Version) ([]index.Item, error) {
	log, err := message.OpenReader(s.Log, s.Offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = log.Close() }()

	return s.ReindexReader(params, log, version)
}

func (s Segment) ReindexReader(params index.Params, log *message.Reader, version index.Version) ([]index.Item, error) {
	var position = log.InitialPosition()
	var indexTime int64
	var newIndex []index.Item
	for {
		msg, nextPosition, err := log.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}

		item := params.NewItem(msg, position, indexTime)
		newIndex = append(newIndex, item)

		position = nextPosition
		indexTime = item.Timestamp
	}

	if err := index.Write(s.Index, s.Offset, version, params, newIndex); err != nil {
		return nil, err
	}
	return newIndex, nil
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

	if err := s.syncDir(); err != nil {
		return fmt.Errorf("backup sync dir: %w", err)
	}

	return nil
}

func (s Segment) Migrate(mversion message.Version, iversion index.Version, params index.Params) error {
	oldLog, err := message.OpenReader(s.Log, s.Offset)
	if err != nil {
		return fmt.Errorf("migrate open reader: %w", err)
	}
	defer func() { _ = oldLog.Close() }()

	if oldLog.Version() == mversion {
		return nil
	}

	// Remove the index first to guarantee its rebuild and migrated
	switch err := os.Remove(s.Index); {
	case errors.Is(err, os.ErrNotExist):
	case err != nil:
		return fmt.Errorf("migrate index remove: %w", err)
	}

	migratedPath := s.Log + ".migrate"
	if err := os.Remove(migratedPath); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("migrate remove stale temp: %w", err)
	}
	migratedLog, err := message.OpenWriter(migratedPath, s.Offset, mversion)
	if err != nil {
		return fmt.Errorf("migrate open writer: %w", err)
	}
	defer func() { _ = migratedLog.Close() }() // ignoring since its only applicable if an error has happened

	var oldPosition = oldLog.InitialPosition()
	var indexTime int64
	var migratedIndex []index.Item
	for {
		msg, nextOldPosition, err := oldLog.Read(oldPosition)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return fmt.Errorf("migrate read: %w", err)
		}

		migratedPosition, err := migratedLog.Write(msg)
		if err != nil {
			return fmt.Errorf("migrate write: %w", err)
		}

		item := params.NewItem(msg, migratedPosition, indexTime)
		migratedIndex = append(migratedIndex, item)
		indexTime = item.Timestamp

		oldPosition = nextOldPosition
	}

	if err := oldLog.Close(); err != nil {
		return fmt.Errorf("migrate close old log: %w", err)
	}
	if err := migratedLog.SyncAndClose(); err != nil {
		return fmt.Errorf("migrate close migrated log: %w", err)
	}

	if err := os.Rename(migratedLog.Path, s.Log); err != nil {
		return fmt.Errorf("migrate log rename: %w", err)
	}
	if err := index.Write(s.Index, s.Offset, iversion, params, migratedIndex); err != nil {
		return fmt.Errorf("migrate index write: %w", err)
	}

	if err := s.syncDir(); err != nil {
		return fmt.Errorf("migrate sync dir: %w", err)
	}

	return nil
}

func (olds Segment) Rename(news Segment) error {
	if err := os.Rename(olds.Log, news.Log); err != nil {
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

	if err := os.Rename(olds.Log, news.Log); err != nil {
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
	if err := os.Remove(s.Log); err != nil {
		return fmt.Errorf("remove log delete: %w", err)
	}
	return nil
}

func (s Segment) syncDir() error {
	if !s.AutoSync {
		return nil
	}
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
	return r.NewAt(lowestOffset)
}

func (s Segment) forRewrite() (*RewriteSegment, error) {
	randStr, err := randStr(5)
	if err != nil {
		return nil, err
	}

	dst := &RewriteSegment{
		Segment: s.NewAt(s.Offset),

		SurviveOffsets: map[int64]struct{}{},
		DeletedOffsets: map[int64]struct{}{},
	}

	dst.Log = fmt.Sprintf("%s.rewrite.%s", s.Log, randStr)
	dst.Index = fmt.Sprintf("%s.rewrite.%s", s.Index, randStr)

	return dst, nil
}

func (src Segment) Rewrite(dropOffsets map[int64]struct{}, params index.Params, mversion message.Version, iversion index.Version) (*RewriteSegment, error) {
	dst, err := src.forRewrite()
	if err != nil {
		return nil, err
	}

	srcLog, err := message.OpenReader(src.Log, src.Offset)
	if err != nil {
		return nil, err
	}
	defer func() { _ = srcLog.Close() }()

	srcVersion := srcLog.Version()

	dstLog, err := message.OpenWriter(dst.Log, dst.Offset, mversion)
	if err != nil {
		return nil, err
	}
	defer func() { _ = dstLog.Close() }() // ignoring since its only applicable if an error has happened

	var srcPosition = srcLog.InitialPosition()
	var indexTime int64
	var dstIndex []index.Item
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
			dst.DeletedSize += message.Size(msg, srcVersion) + params.Size()
		} else {
			dstPosition, err := dstLog.Write(msg)
			if err != nil {
				return nil, err
			}
			dst.SurviveOffsets[msg.Offset] = struct{}{}

			item := params.NewItem(msg, dstPosition, indexTime)
			dstIndex = append(dstIndex, item)
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
	if err := index.Write(dst.Index, dst.Offset, iversion, params, dstIndex); err != nil {
		return nil, err
	}

	if len(dst.SurviveOffsets) > 0 {
		dst.Offset = message.MinOffset(dst.SurviveOffsets)
	}
	dst.Stats, err = dst.Stat(params)
	if err != nil {
		return nil, err
	}
	return dst, nil
}
