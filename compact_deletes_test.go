package klevdb

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/pkg/message"
)

func TestDeletes(t *testing.T) {
	msgs := message.Gen(5)

	t.Run("Empty", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		deletedMsgs, cmp, err := CompactDeletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, deletedMsgs)
		require.Equal(t, int64(0), cmp)
	})

	t.Run("None", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactDeletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, deletedMsgs)
		require.Equal(t, int64(0), cmp)
	})

	t.Run("Dups", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		nmsgs := slices.Clone(msgs)
		for i := range nmsgs {
			nmsgs[i].Value = nil
		}

		_, err = l.Publish(nmsgs)
		require.NoError(t, err)

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactDeletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, deletedMsgs, 5)
		for _, nmsg := range nmsgs {
			require.Contains(t, deletedMsgs, nmsg)
		}
		require.Equal(t, l.Size(nmsgs[0])*int64(len(nmsgs)), cmp)
	})
}
