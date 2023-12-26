package index

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/klev-dev/klevdb/message"
)

var ErrIndexEmpty = fmt.Errorf("%w: no items", message.ErrInvalidOffset)

func Consume(items []Item, offset int64) (int64, int64, error) {
	if len(items) == 0 {
		return 0, 0, ErrIndexEmpty
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
		return 0, 0, message.ErrInvalidOffset
	case offset == endItem.Offset:
		return endItem.Position, endItem.Position, nil
	}

	idx, _ := slices.BinarySearchFunc(items, offset, func(item Item, offset int64) int {
		return cmp.Compare(item.Offset, offset)
	})

	return items[idx].Position, endItem.Position, nil
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

	idx, found := slices.BinarySearchFunc(items, offset, func(item Item, offset int64) int {
		return cmp.Compare(item.Offset, offset)
	})

	if !found {
		return 0, message.ErrNotFound
	}

	return items[idx].Position, nil
}
