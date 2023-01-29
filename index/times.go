package index

import (
	"sort"

	"github.com/klev-dev/klevdb/message"
)

func Time(items []Item, ts int64) (int64, error) {
	if len(items) == 0 {
		return 0, ErrIndexEmpty
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case ts < beginItem.Timestamp:
		return 0, message.ErrInvalidOffset
	case ts == beginItem.Timestamp:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case endItem.Timestamp < ts:
		return 0, message.ErrNotFound
	}

	foundIndex := sort.Search(len(items), func(midIndex int) bool {
		return items[midIndex].Timestamp >= ts
	})
	return items[foundIndex].Position, nil
}
