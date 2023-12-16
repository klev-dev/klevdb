package index

import (
	"fmt"
	"testing"

	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func genItems(offsets ...int64) []NoItem {
	items := make([]NoItem, len(offsets))
	for i, v := range offsets {
		items[i] = NoItem{offset: v, position: v}
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
		{items: nil, offset: 0, err: ErrIndexEmpty},
		// single item tests
		{items: []int64{1}, offset: message.OffsetOldest, position: 1, max: 1},
		{items: []int64{1}, offset: message.OffsetNewest, position: 1, max: 1},
		{items: []int64{1}, offset: 0, position: 1, max: 1},
		{items: []int64{1}, offset: 1, position: 1, max: 1},
		{items: []int64{1}, offset: 2, err: message.ErrInvalidOffset},
		// continuous tests
		{items: []int64{1, 2, 3}, offset: message.OffsetOldest, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: message.OffsetNewest, position: 3, max: 3},
		{items: []int64{1, 2, 3}, offset: 0, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: 1, position: 1, max: 3},
		{items: []int64{1, 2, 3}, offset: 3, position: 3, max: 3},
		{items: []int64{1, 2, 3}, offset: 4, err: message.ErrInvalidOffset},
		// gaps tests
		{items: []int64{1, 3}, offset: message.OffsetOldest, position: 1, max: 3},
		{items: []int64{1, 3}, offset: message.OffsetNewest, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 0, position: 1, max: 3},
		{items: []int64{1, 3}, offset: 1, position: 1, max: 3},
		{items: []int64{1, 3}, offset: 2, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 3, position: 3, max: 3},
		{items: []int64{1, 3}, offset: 4, err: message.ErrInvalidOffset},
		{items: []int64{1, 3, 5}, offset: message.OffsetOldest, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: message.OffsetNewest, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 0, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: 1, position: 1, max: 5},
		{items: []int64{1, 3, 5}, offset: 2, position: 3, max: 5},
		{items: []int64{1, 3, 5}, offset: 3, position: 3, max: 5},
		{items: []int64{1, 3, 5}, offset: 4, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 5, position: 5, max: 5},
		{items: []int64{1, 3, 5}, offset: 6, err: message.ErrInvalidOffset},
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
		{items: nil, offset: 0, err: ErrIndexEmpty},
		// single item tests
		{items: []int64{1}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1}, offset: message.OffsetNewest, position: 1},
		{items: []int64{1}, offset: 0, err: message.ErrNotFound},
		{items: []int64{1}, offset: 1, position: 1},
		{items: []int64{1}, offset: 2, err: message.ErrNotFound},
		// continuous tests
		{items: []int64{1, 2, 3}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 2, 3}, offset: message.OffsetNewest, position: 3},
		{items: []int64{1, 2, 3}, offset: 0, err: message.ErrNotFound},
		{items: []int64{1, 2, 3}, offset: 1, position: 1},
		{items: []int64{1, 2, 3}, offset: 3, position: 3},
		{items: []int64{1, 2, 3}, offset: 4, err: message.ErrNotFound},
		// gaps tests
		{items: []int64{1, 3}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 3}, offset: message.OffsetNewest, position: 3},
		{items: []int64{1, 3}, offset: 0, err: message.ErrNotFound},
		{items: []int64{1, 3}, offset: 1, position: 1},
		{items: []int64{1, 3}, offset: 2, err: message.ErrNotFound},
		{items: []int64{1, 3}, offset: 3, position: 3},
		{items: []int64{1, 3}, offset: 4, err: message.ErrNotFound},
		{items: []int64{1, 3, 5}, offset: message.OffsetOldest, position: 1},
		{items: []int64{1, 3, 5}, offset: message.OffsetNewest, position: 5},
		{items: []int64{1, 3, 5}, offset: 0, err: message.ErrNotFound},
		{items: []int64{1, 3, 5}, offset: 1, position: 1},
		{items: []int64{1, 3, 5}, offset: 2, err: message.ErrNotFound},
		{items: []int64{1, 3, 5}, offset: 3, position: 3},
		{items: []int64{1, 3, 5}, offset: 4, err: message.ErrNotFound},
		{items: []int64{1, 3, 5}, offset: 5, position: 5},
		{items: []int64{1, 3, 5}, offset: 6, err: message.ErrNotFound},
	}

	for _, tc := range tests {
		t.Run(fmt.Sprintf("%v:%d", tc.items, tc.offset), func(t *testing.T) {
			position, err := Get(genItems(tc.items...), tc.offset)
			require.Equal(t, tc.position, position)
			require.Equal(t, tc.err, err)
		})
	}
}
