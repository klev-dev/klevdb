package index

import (
	"testing"

	"github.com/klev-dev/klevdb/message"
	art "github.com/plar/go-adaptive-radix-tree"
	"github.com/stretchr/testify/require"
)

func TestPKeys(t *testing.T) {
	t.Run("Empty", func(t *testing.T) {
		keys := art.New()
		pos, err := Keys(keys, KeyHashEncoded(1))
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Single", func(t *testing.T) {
		item := PKeyItem{position: 1, keyHash: [8]byte{0, 0, 0, 0, 0, 0, 0, 123}}

		keys := art.New()
		PAppendKeys(keys, []PKeyItem{item})

		pos, err := Keys(keys, item.keyHash[:])
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item.position}, pos)

		pos, err = Keys(keys, []byte{0, 0, 0, 0, 0, 0, 0, 233})
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})

	t.Run("Duplicate", func(t *testing.T) {
		item1 := PKeyItem{position: 1, keyHash: [8]byte{0, 0, 0, 0, 0, 0, 0, 123}}
		item2 := PKeyItem{position: 2, keyHash: [8]byte{0, 0, 0, 0, 0, 0, 0, 123}}
		item3 := PKeyItem{position: 3, keyHash: [8]byte{0, 0, 0, 0, 0, 0, 0, 213}}

		keys := art.New()
		PAppendKeys(keys, []PKeyItem{item1, item2, item3})

		pos, err := Keys(keys, item1.keyHash[:])
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item1.position, item2.position}, pos)

		pos, err = Keys(keys, item3.keyHash[:])
		require.NoError(t, err)
		require.ElementsMatch(t, []int64{item3.position}, pos)

		pos, err = Keys(keys, []byte{0, 0, 0, 0, 0, 0, 0, 233})
		require.ErrorIs(t, message.ErrNotFound, err)
		require.Empty(t, pos)
	})
}
