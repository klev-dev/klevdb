package index

import (
	"fmt"
	"testing"

	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func TestTime(t *testing.T) {
	gen := func(ts ...int64) []TimeItem {
		items := make([]TimeItem, len(ts))
		for i := range items {
			items[i].timestamp = ts[i]
			items[i].position = int64(i)
		}
		return items
	}

	t.Run("Empty", func(t *testing.T) {
		items := gen()
		_, err := Time(items, 1)
		require.ErrorIs(t, ErrIndexEmpty, err)
	})

	t.Run("Before", func(t *testing.T) {
		items := gen(1)
		_, err := Time(items, 0)
		require.ErrorIs(t, message.ErrInvalidOffset, err)
	})

	t.Run("After", func(t *testing.T) {
		items := gen(1)
		_, err := Time(items, 2)
		require.ErrorIs(t, message.ErrNotFound, err)
	})

	t.Run("Exact", func(t *testing.T) {
		for i := 1; i < 6; i++ {
			itemsProto := make([]int64, i)
			for k := range itemsProto {
				itemsProto[k] = int64(k + 1)
			}
			items := gen(itemsProto...)
			for m, it := range itemsProto {
				t.Run(fmt.Sprintf("%d/%d", i, it), func(t *testing.T) {
					pos, err := Time(items, it)
					require.NoError(t, err)
					require.Equal(t, int64(m), pos)
				})
			}
		}
	})

	t.Run("RepeatSingle", func(t *testing.T) {
		for i := 1; i < 6; i++ {
			itemsProto := make([]int64, i)
			for k := range itemsProto {
				itemsProto[k] = 1
			}
			items := gen(itemsProto...)
			t.Run(fmt.Sprintf("Exact/%d", i), func(t *testing.T) {
				pos, err := Time(items, 1)
				require.NoError(t, err)
				require.Equal(t, int64(0), pos)
			})
		}
	})

	t.Run("RepeatMulti", func(t *testing.T) {
		for i := 1; i < 6; i++ {
			itemsProto := make([]int64, i*3)
			for k := 0; k < i; k++ {
				itemsProto[k] = 1
				itemsProto[k+i] = 3
				itemsProto[k+i*2] = 5
			}
			items := gen(itemsProto...)

			t.Run(fmt.Sprintf("Start/%d", i), func(t *testing.T) {
				pos, err := Time(items, 1)
				require.NoError(t, err)
				require.Equal(t, int64(0), pos)
			})

			t.Run(fmt.Sprintf("Mid/%d", i), func(t *testing.T) {
				pos, err := Time(items, 3)
				require.NoError(t, err)
				require.Equal(t, int64(i), pos)
			})

			t.Run(fmt.Sprintf("End/%d", i), func(t *testing.T) {
				pos, err := Time(items, 5)
				require.NoError(t, err)
				require.Equal(t, int64(i*2), pos)
			})

			t.Run(fmt.Sprintf("RelLow/%d", i), func(t *testing.T) {
				pos, err := Time(items, 2)
				require.NoError(t, err)
				require.Equal(t, int64(i), pos)
			})

			t.Run(fmt.Sprintf("RelHigh/%d", i), func(t *testing.T) {
				pos, err := Time(items, 4)
				require.NoError(t, err)
				require.Equal(t, int64(i*2), pos)
			})
		}
	})
}
