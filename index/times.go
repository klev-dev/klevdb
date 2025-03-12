package index

import (
	"errors"
	"fmt"
	"sort"

	"github.com/klev-dev/klevdb/message"
)

var ErrTimeIndexEmpty = fmt.Errorf("%w: no time items", message.ErrInvalidOffset)
var ErrTimeBeforeStart = errors.New("time before start")
var ErrTimeAfterEnd = fmt.Errorf("%w: time after end", message.ErrInvalidOffset)

func Time(items []Item, ts int64) (int64, error) {
	if len(items) == 0 {
		return 0, ErrTimeIndexEmpty
	}

	beginIndex := 0
	beginItem := items[beginIndex]
	switch {
	case ts < beginItem.Timestamp:
		return 0, ErrTimeBeforeStart
	case ts == beginItem.Timestamp:
		return beginItem.Position, nil
	}

	endIndex := len(items) - 1
	endItem := items[endIndex]
	switch {
	case endItem.Timestamp < ts:
		return 0, ErrTimeAfterEnd
	}

	foundIndex := sort.Search(len(items), func(midIndex int) bool {
		return items[midIndex].Timestamp >= ts
	})
	return items[foundIndex].Position, nil
}
