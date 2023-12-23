package index

import (
	"fmt"
	"testing"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/stretchr/testify/require"
)

func TestKeys(t *testing.T) {
	t.Run("Hash", func(t *testing.T) {
		hash := KeyHash([]byte("a"))
		fmt.Println("hash:", hash)
	})
	t.Run("Empty", func(t *testing.T) {
		keys := art.New()
		pos, err := Keys(keys, KeyHash([]byte("a")))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Single", func(t *testing.T) {
		item := Item{Position: 1, KeyHash: KeyHash([]byte("a"))}
		fmt.Println("item:", item)

		keys := art.New()
		AppendKeys(keys, []Item{item})
		fmt.Println(keys)

		pos, err := Keys(keys, item.KeyHash)
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item.Position}, pos)

		pos, err = Keys(keys, KeyHash([]byte("c")))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Duplicate", func(t *testing.T) {
		item1 := Item{Position: 1, KeyHash: KeyHash([]byte("a"))}
		item2 := Item{Position: 2, KeyHash: KeyHash([]byte("a"))}
		item3 := Item{Position: 3, KeyHash: KeyHash([]byte("b"))}

		keys := art.New()
		AppendKeys(keys, []Item{item1, item2, item3})

		pos, err := Keys(keys, item1.KeyHash)
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item1.Position, item2.Position}, pos)

		pos, err = Keys(keys, item3.KeyHash)
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item3.Position}, pos)

		pos, err = Keys(keys, KeyHash([]byte("c")))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})
}
