package index

import (
	"fmt"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

var ErrIndexEmpty = fmt.Errorf("%w: no items", message.ErrInvalidOffset)

func oldest(items []Item) (int64, error) {
	if len(items) == 0 {
		return 0, kleverr.Newf("%w: oldest is missing", ErrIndexEmpty)
	}
	return items[0].Position, nil
}

func newest(items []Item) (int64, error) {
	if len(items) == 0 {
		return 0, kleverr.Newf("%w: newest is missing", ErrIndexEmpty)
	}
	return items[len(items)-1].Position, nil
}

func Consume(items []Item, offset int64) (int64, error) {
	switch offset {
	case message.OffsetOldest:
		return oldest(items)
	case message.OffsetNewest:
		return newest(items)
	}

	if len(items) == 0 {
		return 0, kleverr.Newf("%w: %d is missing", ErrIndexEmpty, offset)
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset <= beginItem.Offset:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.Offset:
		return 0, kleverr.Newf("%w %d: after end", message.ErrInvalidOffset, offset)
	case offset == endItem.Offset:
		return endItem.Position, nil
	}

	for beginIndex <= endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midItem := items[midIndex]
		switch {
		case midItem.Offset < offset:
			beginIndex = midIndex + 1
		case midItem.Offset > offset:
			endIndex = midIndex - 1
		default:
			return midItem.Position, nil
		}
	}

	return items[beginIndex].Position, nil
}

func Get(items []Item, offset int64) (int64, error) {
	switch offset {
	case message.OffsetOldest:
		return oldest(items)
	case message.OffsetNewest:
		return newest(items)
	}

	if len(items) == 0 {
		return 0, kleverr.Newf("%w: %d is missing", ErrIndexEmpty, offset)
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset < beginItem.Offset:
		return 0, kleverr.Newf("%w: offset %d is before begin", message.ErrNotFound, offset)
	case offset == beginItem.Offset:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.Offset:
		return 0, kleverr.Newf("%w: offset %d is after end", message.ErrNotFound, offset)
	case offset == endItem.Offset:
		return endItem.Position, nil
	}

	for beginIndex <= endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midItem := items[midIndex]
		switch {
		case midItem.Offset < offset:
			beginIndex = midIndex + 1
		case midItem.Offset > offset:
			endIndex = midIndex - 1
		default:
			return midItem.Position, nil
		}
	}

	return 0, kleverr.Newf("%w: offset %d not found", message.ErrNotFound, offset)
}
