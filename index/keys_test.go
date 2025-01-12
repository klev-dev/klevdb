package index

import (
	"testing"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree/v2"
	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		keys := art.New()
		pos, err := Keys(keys, KeyHashEncoded(1))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Single", func(t *testing.T) {
		item := Item{Position: 1, KeyHash: 123}

		keys := art.New()
		AppendKeys(keys, []Item{item})

		pos, err := Keys(keys, KeyHashEncoded(item.KeyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item.Position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(321))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Duplicate", func(t *testing.T) {
		item1 := Item{Position: 1, KeyHash: 123}
		item2 := Item{Position: 2, KeyHash: 123}
		item3 := Item{Position: 3, KeyHash: 213}

		keys := art.New()
		AppendKeys(keys, []Item{item1, item2, item3})

		pos, err := Keys(keys, KeyHashEncoded(item1.KeyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item1.Position, item2.Position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(item3.KeyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item3.Position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(321))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})
}
