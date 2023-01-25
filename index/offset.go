package index

import (
	"fmt"

	"github.com/klev-dev/klevdb/message"
)

var ErrIndexEmpty = fmt.Errorf("%w: no items", message.ErrInvalidOffset)

func Consume(items []Item, offset int64) (int64, error) {
	if len(items) == 0 {
		return 0, ErrIndexEmpty
	}

	switch offset {
	case message.OffsetOldest:
		return items[0].Position, nil
	case message.OffsetNewest:
		return items[len(items)-1].Position, nil
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
		return 0, message.ErrInvalidOffset
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
	if len(items) == 0 {
		return 0, ErrIndexEmpty
	}

	switch offset {
	case message.OffsetOldest:
		return items[0].Position, nil
	case message.OffsetNewest:
		return items[len(items)-1].Position, nil
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset < beginItem.Offset:
		return 0, message.ErrNotFound
	case offset == beginItem.Offset:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.Offset:
		return 0, message.ErrNotFound
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

	return 0, message.ErrNotFound
}
