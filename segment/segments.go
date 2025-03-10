package segment

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/kleverr"
)

func Find(dir string) ([]Segment, error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, kleverr.Newf("read dir: %w", err)
	}

	var segments []Segment
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			offsetStr := strings.TrimSuffix(f.Name(), ".log")

			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, kleverr.Newf("parse offset: %w", err)
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
			return Stats{}, err
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
		s := segments[len(segments)-1]
		return s.Check(params)
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
		s := segments[len(segments)-1]
		return s.Recover(params)
	}
}

func BackupDir(dir, target string) error {
	segments, err := Find(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	}

	if err := os.MkdirAll(target, 0700); err != nil {
		return kleverr.Newf("backup create dir: %w", err)
	}

	return Backup(segments, target)
}

func Backup(segments []Segment, dir string) error {
	for _, seg := range segments {
		if err := seg.Backup(dir); err != nil {
			return err
		}
	}

	return nil
}
