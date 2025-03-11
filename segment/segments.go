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
		return nil, fmt.Errorf("find read dir: %w", err)
	}

	var segments []Segment
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			offsetStr := strings.TrimSuffix(f.Name(), ".log")

			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("find parse offset: %w", err)
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
