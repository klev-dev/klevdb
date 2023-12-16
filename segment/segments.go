package segment

import (
	"errors"
	"os"
	"strconv"
	"strings"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/kleverr"
)

func Find[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir string) ([]Segment[IX, IT, IC, IS], error) {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil, kleverr.Newf("could not list dir: %w", err)
	}

	var segments []Segment[IX, IT, IC, IS]
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".log") {
			offsetStr := strings.TrimSuffix(f.Name(), ".log")

			offset, err := strconv.ParseInt(offsetStr, 10, 64)
			if err != nil {
				return nil, kleverr.Newf("parse offset failed: %w", err)
			}

			segments = append(segments, New[IX, IT](dir, offset))
		}
	}

	return segments, nil
}

func StatDir[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir string, ix IX) (Stats, error) {
	segments, err := Find[IX, IT, IC, IS](dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return Stats{}, nil
	case err != nil:
		return Stats{}, err
	}
	return Stat(segments, ix)
}

func Stat[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](segments []Segment[IX, IT, IC, IS], ix IX) (Stats, error) {
	var total = Stats{}
	for _, seg := range segments {
		segStat, err := seg.Stat(ix)
		if err != nil {
			return Stats{}, err
		}

		total.Segments += segStat.Segments
		total.Messages += segStat.Messages
		total.Size += segStat.Size
	}
	return total, nil
}

func CheckDir[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir string, ix IX) error {
	switch segments, err := Find[IX, IT, IC, IS](dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(segments) == 0:
		return nil
	default:
		s := segments[len(segments)-1]
		return s.Check(ix)
	}
}

func RecoverDir[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir string, ix IX) error {
	switch segments, err := Find[IX, IT, IC, IS](dir); {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	case len(segments) == 0:
		return nil
	default:
		s := segments[len(segments)-1]
		return s.Recover(ix)
	}
}

func BackupDir[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](dir, target string) error {
	segments, err := Find[IX, IT, IC, IS](dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return err
	}

	if err := os.MkdirAll(target, 0700); err != nil {
		return kleverr.Newf("could not create backup dir: %w", err)
	}

	return Backup(segments, target)
}

func Backup[IX index.Index[IT, IC, IS], IT index.IndexItem, IC index.IndexContext, IS index.IndexStore](segments []Segment[IX, IT, IC, IS], dir string) error {
	for _, seg := range segments {
		if err := seg.Backup(dir); err != nil {
			return kleverr.Newf("could not backup segment %d: %w", seg.Offset, err)
		}
	}

	return nil
}
