package index

import (
	"testing"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
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
		item := KeyItem{position: 1, keyHash: 123}

		keys := art.New()
		AppendKeys(keys, []KeyItem{item})

		pos, err := Keys(keys, KeyHashEncoded(item.keyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item.position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(321))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Duplicate", func(t *testing.T) {
		item1 := KeyItem{position: 1, keyHash: 123}
		item2 := KeyItem{position: 2, keyHash: 123}
		item3 := KeyItem{position: 3, keyHash: 213}

		keys := art.New()
		AppendKeys(keys, []KeyItem{item1, item2, item3})

		pos, err := Keys(keys, KeyHashEncoded(item1.keyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item1.position, item2.position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(item3.keyHash))
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item3.position}, pos)

		pos, err = Keys(keys, KeyHashEncoded(321))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})
}
