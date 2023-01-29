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

func TestUpdates(t *testing.T) {
	msgs := message.Gen(5)

	t.Run("Empty", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		off, cmp, err := Updates(context.TODO(), l, time.Now())
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

		off, cmp, err := Updates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, off)
		require.Equal(t, int64(0), cmp)

		gmsg, err := l.GetByKey(msgs[0].Key)
		require.NoError(t, err)
		require.Equal(t, msgs[0], gmsg)
	})

	t.Run("First", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		dmsgs := []message.Message{msgs[0]}
		dmsgs[0].Value = []byte("abc")
		_, err = l.Publish(dmsgs)
		require.NoError(t, err)

		off, cmp, err := Updates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, off, 1)
		require.Contains(t, off, int64(0))
		require.Equal(t, l.Size(msgs[0]), cmp)

		gmsg, err := l.GetByKey(msgs[0].Key)
		require.NoError(t, err)
		require.Equal(t, dmsgs[0], gmsg)
	})

	t.Run("Last", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		dmsgs := []message.Message{msgs[4]}
		dmsgs[0].Value = []byte("abc")
		_, err = l.Publish(dmsgs)
		require.NoError(t, err)

		off, cmp, err := Updates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, off, 1)
		require.Contains(t, off, int64(4))
		require.Equal(t, l.Size(msgs[0]), cmp)

		gmsg, err := l.GetByKey(msgs[4].Key)
		require.NoError(t, err)
		require.Equal(t, dmsgs[0], gmsg)
	})

	t.Run("Multi", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		off, cmp, err := Updates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, off, len(msgs))
		for i := range msgs {
			require.Contains(t, off, int64(i))
		}
		require.Equal(t, l.Size(msgs[0])*int64(len(msgs)), cmp)

		gmsg, err := l.GetByKey(msgs[1].Key)
		require.NoError(t, err)
		require.Equal(t, msgs[1], gmsg)
	})

	t.Run("Time", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		nmsgs := slices.Clone(msgs)
		for i := range nmsgs {
			nmsgs[i].Time = nmsgs[i].Time.Add(time.Hour)
		}
		_, err = l.Publish(nmsgs)
		require.NoError(t, err)

		off, cmp, err := Updates(context.TODO(), l, nmsgs[2].Time)
		require.NoError(t, err)
		require.Len(t, off, 3)
		for i := 0; i < 3; i++ {
			require.Contains(t, off, int64(i))
		}
		require.Equal(t, l.Size(msgs[0])*3, cmp)

		gmsg, err := l.GetByKey(msgs[1].Key)
		require.NoError(t, err)
		require.Equal(t, nmsgs[1], gmsg)
	})

	t.Run("NilKey", func(t *testing.T) {
		l, err := klevdb.Open(t.TempDir(), klevdb.Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish([]klevdb.Message{
			{Key: []byte("x")},
			{},
			{},
		})
		require.NoError(t, err)

		off, cmp, err := Updates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, off, 1)
		require.Contains(t, off, int64(1))
		require.Equal(t, l.Size(klevdb.Message{}), cmp)
	})
}
