package index

import (
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

func Time(items []Item, ts int64) (int64, error) {
	if len(items) == 0 {
		return 0, kleverr.Newf("%w: %d not found", ErrIndexEmpty, ts)
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case ts < beginItem.Timestamp:
		return 0, kleverr.Newf("%w: ts before start", message.ErrNotFound)
	case ts == beginItem.Timestamp:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case endItem.Timestamp < ts:
		return 0, kleverr.Newf("%w: ts after end", message.ErrInvalidOffset)
	case endItem.Timestamp == ts:
		return endItem.Position, nil
	}

	for beginIndex <= endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midItem := items[midIndex]
		switch {
		case midItem.Timestamp < ts:
			beginIndex = midIndex + 1
		case midItem.Timestamp > ts:
			endIndex = midIndex - 1
		default:
			return midItem.Position, nil
		}
	}

	return items[beginIndex].Position, nil
}
