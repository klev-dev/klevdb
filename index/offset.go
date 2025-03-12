package index

import (
	"fmt"

	"github.com/klev-dev/klevdb/message"
)

var ErrOffsetIndexEmpty = fmt.Errorf("%w: no offset items", message.ErrInvalidOffset)
var ErrOffsetBeforeStart = fmt.Errorf("%w: offset before start", message.ErrNotFound)
var ErrOffsetAfterEnd = fmt.Errorf("%w: offset after end", message.ErrInvalidOffset)
var ErrOffsetNotFound = fmt.Errorf("%w: offset not found", message.ErrNotFound)

func Consume(items []Item, offset int64) (int64, int64, error) {
	if len(items) == 0 {
		return 0, 0, ErrOffsetIndexEmpty
	}

	switch offset {
	case message.OffsetOldest:
		return items[0].Position, items[len(items)-1].Position, nil
	case message.OffsetNewest:
		last := items[len(items)-1]
		return last.Position, last.Position, nil
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset <= beginItem.Offset:
		return beginItem.Position, items[len(items)-1].Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.Offset:
		return 0, 0, ErrOffsetAfterEnd
	case offset == endItem.Offset:
		return endItem.Position, endItem.Position, nil
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
			return midItem.Position, endItem.Position, nil
		}
	}

	return items[beginIndex].Position, endItem.Position, nil
}

func Get(items []Item, offset int64) (int64, error) {
	if len(items) == 0 {
		return 0, ErrOffsetIndexEmpty
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
		return 0, ErrOffsetBeforeStart
	case offset == beginItem.Offset:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.Offset:
		return 0, ErrOffsetAfterEnd
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

	return 0, ErrOffsetNotFound
}
