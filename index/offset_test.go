package index

import (
	"fmt"
	"testing"

	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func genItems(offsets ...int64) []Item {
	items := make([]Item, len(offsets))
	for i, v := range offsets {
		items[i] = Item{Offset: v, Position: v}
	}
	return items
}

func TestConsume(t *testing.T) {
	var tests = []struct {
		items    []int64
		offset   int64
		position int64
		max      int64
		err      error
	}{
		// empty tests
		{items: nil, offset: 0, err: ErrOffsetIndexEmpty},
		// single item tests
		{items: []int64{1}, offset: message.OffsetOldest, position: 1, max: 1},
		{items: []int64{1}, offset: message.OffsetNewest, position: 1, max: 1},
		{items: []int64{1}, offset: 0, position: 1, max: 1},
		{items: []int64{1}, offset: 1, position: 1, max: 1},
		{items: []int64{1}, offset: 2, err: ErrOffsetAfterEnd},
		// continuous tests
		{items: []int64{1, 2, 3}, offset: message.OffsetOldest, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: message.OffsetNewest, position: 3, max: 3},
		{items: []int64{1, 2, 3}, offset: 0, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: 1, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: 3, position: 3, max: 3},
		{items: []int64{1, 2, 3}, offset: 4, err: ErrOffsetAfterEnd},
		// gaps tests
		{items: []int64{1, 3}, offset: message.OffsetOldest, position: 1, max: 3},
		{items: []int64{1, 3}, offset: message.OffsetNewest, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 0, position: 1, max: 3},
		{items: []int64{1, 3}, offset: 1, position: 1, max: 3},
		{items: []int64{1, 3}, offset: 2, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 3, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 4, err: ErrOffsetAfterEnd},
		{items: []int64{1, 3, 5}, offset: message.OffsetOldest, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: message.OffsetNewest, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 0, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: 1, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: 2, position: 3, max: 5},
		{items: []int64{1, 3, 5}, offset: 3, position: 3, max: 5},
		{items: []int64{1, 3, 5}, offset: 4, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 5, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 6, err: ErrOffsetAfterEnd},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v:%d", tc.items, tc.offset), func(t *testing.T) {
			position, maxPosition, err := Consume(genItems(tc.items...), tc.offset)
			require.Equal(t, tc.position, position)
			require.Equal(t, tc.max, maxPosition)
			require.Equal(t, tc.err, err)
		})
	}
}

func TestGet(t *testing.T) {
	var tests = []struct {
		items    []int64
		offset   int64
		position int64
		err      error
	}{
		// empty tests
		{items: nil, offset: 0, err: ErrOffsetIndexEmpty},
		// single item tests
		{items: []int64{1}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1}, offset: message.OffsetNewest, position: 1},
		{items: []int64{1}, offset: 0, err: ErrOffsetBeforeStart},
		{items: []int64{1}, offset: 1, position: 1},
		{items: []int64{1}, offset: 2, err: ErrOffsetAfterEnd},
		// continuous tests
		{items: []int64{1, 2, 3}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 2, 3}, offset: message.OffsetNewest, position: 3},
		{items: []int64{1, 2, 3}, offset: 0, err: ErrOffsetBeforeStart},
		{items: []int64{1, 2, 3}, offset: 1, position: 1},
		{items: []int64{1, 2, 3}, offset: 3, position: 3},
		{items: []int64{1, 2, 3}, offset: 4, err: ErrOffsetAfterEnd},
		// gaps tests
		{items: []int64{1, 3}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 3}, offset: message.OffsetNewest, position: 3},
		{items: []int64{1, 3}, offset: 0, err: ErrOffsetBeforeStart},
		{items: []int64{1, 3}, offset: 1, position: 1},
		{items: []int64{1, 3}, offset: 2, err: ErrOffsetNotFound},
		{items: []int64{1, 3}, offset: 3, position: 3},
		{items: []int64{1, 3}, offset: 4, err: ErrOffsetAfterEnd},
		{items: []int64{1, 3, 5}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 3, 5}, offset: message.OffsetNewest, position: 5},
		{items: []int64{1, 3, 5}, offset: 0, err: ErrOffsetBeforeStart},
		{items: []int64{1, 3, 5}, offset: 1, position: 1},
		{items: []int64{1, 3, 5}, offset: 2, err: ErrOffsetNotFound},
		{items: []int64{1, 3, 5}, offset: 3, position: 3},
		{items: []int64{1, 3, 5}, offset: 4, err: ErrOffsetNotFound},
		{items: []int64{1, 3, 5}, offset: 5, position: 5},
		{items: []int64{1, 3, 5}, offset: 6, err: ErrOffsetAfterEnd},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v:%d", tc.items, tc.offset), func(t *testing.T) {
			position, err := Get(genItems(tc.items...), tc.offset)
			require.Equal(t, tc.position, position)
			require.Equal(t, tc.err, err)
		})
	}
}
