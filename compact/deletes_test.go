package compact

import (
	"context"
	"testing"
	"time"

	"github.com/klev-dev/klevdb"
	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/slices"
)

func TestDeletes(t *testing.T) {
	msgs := message.Gen(5)

	t.Run("Empty", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		off, cmp, err := Deletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, off)
		require.Equal(t, int64(0), cmp)
	})

	t.Run("None", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		off, cmp, err := Deletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, off)
		require.Equal(t, int64(0), cmp)
	})

	t.Run("Dups", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
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

		off, cmp, err := Deletes(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, off, 5)
		for i := range nmsgs {
			require.Contains(t, off, int64(i))
		}
		require.Equal(t, l.Size(nmsgs[0])*int64(len(nmsgs)), cmp)
	})
}
