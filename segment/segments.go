package segment

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/pkg/kdir"
)

func Find(dir string) ([]Segment, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("find read dir: %w", err)
	}

	// os.ReadDir returns sorted entries; for the same numeric offset, .log ('l')
	// sorts before .segment ('s'). We track seen offsets so that if both exist
	// (e.g. after a partial migration), the .segment entry wins.
	var segments []Segment
	seen := make(map[int64]int) // offset -> index in segments slice

	for _, f := range files {
		if offsetStr, ok := strings.CutSuffix(f.Name(), ".segment"); ok {
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("find parse offset: %w", err)
			}

			if i, exists := seen[offset]; exists {
				segments[i] = New(dir, offset, message.FormatSegment) // upgrade .log entry
			} else {
				seen[offset] = len(segments)
				segments = append(segments, New(dir, offset, message.FormatSegment))
			}
		} else if offsetStr, ok := strings.CutSuffix(f.Name(), ".log"); ok {
			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("find parse offset: %w", err)
			}

			if _, exists := seen[offset]; !exists {
				seen[offset] = len(segments)
				segments = append(segments, New(dir, offset, message.FormatLog))
			}
		}
	}

	return segments, nil
}

func CleanupOrphanLogs(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("cleanup read dir: %w", err)
	}

	var removed bool
	for _, f := range files {
		offsetStr, ok := strings.CutSuffix(f.Name(), ".segment")
		if !ok {
			continue
		}
		orphan := filepath.Join(dir, offsetStr+".log")
		switch err := os.Remove(orphan); {
		case err == nil:
			removed = true
		case errors.Is(err, os.ErrNotExist):
		default:
			return fmt.Errorf("cleanup orphan log: %w", err)
		}
	}

	if removed {
		if err := kdir.Sync(dir); err != nil {
			return fmt.Errorf("cleanup sync dir: %w", err)
		}
	}
	return nil
}

func StatDir(dir string, params index.Params) (Stats, error) {
	segments, err := Find(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return Stats{}, nil
	case err != nil:
		return Stats{}, err
	}
	return Stat(segments, params)
}

func Stat(segments []Segment, params index.Params) (Stats, error) {
	var total = Stats{}
	for _, seg := range segments {
		segStat, err := seg.Stat(params)
		if err != nil {
			return Stats{}, fmt.Errorf("stat %d: %w", seg.Offset, err)
		}

		total.Segments += segStat.Segments
		total.Messages += segStat.Messages
		total.Size += segStat.Size
	}
	return total, nil
}

func CheckDir(dir string, params index.Params) error {
	switch segments, err := Find(dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(segments) == 0:
		return nil
	default:
		seg := segments[len(segments)-1]
		if err := seg.Check(params); err != nil {
			return fmt.Errorf("check %d: %w", seg.Offset, err)
		}
		return nil
	}
}

func RecoverDir(dir string, params index.Params) error {
	switch segments, err := Find(dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(segments) == 0:
		return nil
	default:
		seg := segments[len(segments)-1]
		if err := seg.Recover(params); err != nil {
			return fmt.Errorf("recover %d: %w", seg.Offset, err)
		}
		return nil
	}
}

func MigrateDir(dir string, params index.Params) error {
	switch segments, err := Find(dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(segments) == 0:
		return nil
	default:
		for _, seg := range segments {
			if _, err := seg.Migrate(); err != nil {
				return err
			}
		}
		return CleanupOrphanLogs(dir)
	}
}

func BackupDir(dir, target string) error {
	switch segments, err := Find(dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	default:
		if err := os.MkdirAll(target, 0700); err != nil {
			return fmt.Errorf("backup dir create: %w", err)
		}

		return Backup(segments, target)
	}
}

func Backup(segments []Segment, dir string) error {
	for _, seg := range segments {
		if err := seg.Backup(dir); err != nil {
			return fmt.Errorf("backup %d: %w", seg.Offset, err)
		}
	}

	return nil
}
