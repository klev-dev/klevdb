package index

import (
	"fmt"

	"github.com/klev-dev/klevdb/message"
)

var ErrIndexEmpty = fmt.Errorf("%w: no items", message.ErrInvalidOffset)

func Consume(items []Item, offset int64) (int64, int64, error) {
	if len(items) == 0 {
		return 0, 0, ErrIndexEmpty
	}

	switch offset {
	case message.OffsetOldest:
		return items[0].position, items[len(items)-1].position, nil
	case message.OffsetNewest:
		last := items[len(items)-1]
		return last.position, last.position, nil
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset <= beginItem.offset:
		return beginItem.position, items[len(items)-1].position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.offset:
		return 0, 0, message.ErrInvalidOffset
	case offset == endItem.offset:
		return endItem.position, endItem.position, nil
	}

	for beginIndex <= endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midItem := items[midIndex]
		switch {
		case midItem.offset < offset:
			beginIndex = midIndex + 1
		case midItem.offset > offset:
			endIndex = midIndex - 1
		default:
			return midItem.position, endItem.position, nil
		}
	}

	return items[beginIndex].position, endItem.position, nil
}

func Get(items []Item, offset int64) (int64, error) {
	if len(items) == 0 {
		return 0, ErrIndexEmpty
	}

	switch offset {
	case message.OffsetOldest:
		return items[0].position, nil
	case message.OffsetNewest:
		return items[len(items)-1].position, nil
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case offset < beginItem.offset:
		return 0, message.ErrNotFound
	case offset == beginItem.offset:
		return beginItem.position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case offset > endItem.offset:
		return 0, message.ErrNotFound
	case offset == endItem.offset:
		return endItem.position, nil
	}

	for beginIndex <= endIndex {
		midIndex := (beginIndex + endIndex) / 2
		midItem := items[midIndex]
		switch {
		case midItem.offset < offset:
			beginIndex = midIndex + 1
		case midItem.offset > offset:
			endIndex = midIndex - 1
		default:
			return midItem.position, nil
		}
	}

	return 0, message.ErrNotFound
}
