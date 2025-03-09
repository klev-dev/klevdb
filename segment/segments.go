package segment

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/klev-dev/klevdb/index"
)

func Find(dir string) ([]Segment, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("[segment.Find] %s read: %w", dir, err)
	}

	var segments []Segment
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			offsetStr := strings.TrimSuffix(f.Name(), ".log")

			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("[segment.Find] %s parse offset: %w", f.Name(), err)
			}

			segments = append(segments, New(dir, offset))
		}
	}

	return segments, nil
}

func StatDir(dir string, params index.Params) (Stats, error) {
	segments, err := Find(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return Stats{}, nil
	case err != nil:
		return Stats{}, fmt.Errorf("[segment.StatDir] %s find: %w", dir, err)
	}
	return Stat(segments, params)
}

func Stat(segments []Segment, params index.Params) (Stats, error) {
	var total = Stats{}
	for _, seg := range segments {
		segStat, err := seg.Stat(params)
		if err != nil {
			return Stats{}, fmt.Errorf("[segment.Stat] %d stat: %w", seg.Offset, err)
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
		return fmt.Errorf("[segment.CheckDir] %s find: %w", dir, err)
	case len(segments) == 0:
		return nil
	default:
		s := segments[len(segments)-1]
		if err := s.Check(params); err != nil {
			return fmt.Errorf("[segment.CheckDir] %s check: %w", dir, err)
		}
		return nil
	}
}

func RecoverDir(dir string, params index.Params) error {
	switch segments, err := Find(dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("[segment.RecoverDir] %s find: %w", dir, err)
	case len(segments) == 0:
		return nil
	default:
		s := segments[len(segments)-1]
		if err := s.Recover(params); err != nil {
			return fmt.Errorf("[segment.RecoverDir] %s recover: %w", dir, err)
		}
		return nil
	}
}

func BackupDir(dir, target string) error {
	segments, err := Find(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("[segment.BackupDir] %s find: %w", dir, err)
	}

	if err := os.MkdirAll(target, 0700); err != nil {
		return fmt.Errorf("[segment.BackupDir] %s mkdir: %w", target, err)
	}

	if err := Backup(segments, target); err != nil {
		return fmt.Errorf("[segment.BackupDir] %s backup to %s: %w", dir, target, err)
	}
	return nil
}

func Backup(segments []Segment, target string) error {
	for _, seg := range segments {
		if err := seg.Backup(target); err != nil {
			return fmt.Errorf("[segment.Backup] %s backup of %d: %w", target, seg.Offset, err)
		}
	}

	return nil
}
