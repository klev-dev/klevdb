package klevdb

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/pkg/message"
)

func TestUpdates(t *testing.T) {
	msgs := message.Gen(5)

	t.Run("Empty", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		off, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, off)
		require.Equal(t, int64(0), cmp)
	})

	t.Run("None", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Empty(t, deletedMsgs)
		require.Equal(t, int64(0), cmp)

		gmsg, err := l.GetByKey(msgs[0].Key)
		require.NoError(t, err)
		require.Equal(t, msgs[0], gmsg)
	})

	t.Run("First", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		dmsgs := []Message{msgs[0]}
		dmsgs[0].Value = []byte("abc")
		_, err = l.Publish(dmsgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, deletedMsgs, 1)
		require.Contains(t, deletedMsgs, msgs[0])
		require.Equal(t, l.Size(msgs[0]), cmp)

		gmsg, err := l.GetByKey(msgs[0].Key)
		require.NoError(t, err)
		require.Equal(t, dmsgs[0], gmsg)
	})

	t.Run("Last", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		dmsgs := []Message{msgs[4]}
		dmsgs[0].Value = []byte("abc")
		_, err = l.Publish(dmsgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, deletedMsgs, 1)
		require.Contains(t, deletedMsgs, msgs[4])
		require.Equal(t, l.Size(msgs[0]), cmp)

		gmsg, err := l.GetByKey(msgs[4].Key)
		require.NoError(t, err)
		require.Equal(t, dmsgs[0], gmsg)
	})

	t.Run("Multi", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		_, err = l.Publish(msgs)
		require.NoError(t, err)

		upmsgs := slices.Clone(msgs)
		_, err = l.Publish(upmsgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, deletedMsgs, len(msgs))
		for _, msg := range msgs {
			require.Contains(t, deletedMsgs, msg)
		}
		require.Equal(t, l.Size(msgs[0])*int64(len(msgs)), cmp)

		gmsg, err := l.GetByKey(msgs[1].Key)
		require.NoError(t, err)
		require.Equal(t, upmsgs[1], gmsg)
	})

	t.Run("Time", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
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

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, nmsgs[2].Time)
		require.NoError(t, err)
		require.Len(t, deletedMsgs, 3)
		for i := range 3 {
			require.Contains(t, deletedMsgs, msgs[i])
		}
		require.Equal(t, l.Size(msgs[0])*3, cmp)

		gmsg, err := l.GetByKey(msgs[1].Key)
		require.NoError(t, err)
		require.Equal(t, nmsgs[1], gmsg)
	})

	t.Run("NilKey", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{KeyIndex: true})
		require.NoError(t, err)
		defer l.Close()

		nmsgs := message.Gen(3)
		nmsgs[1].Key = nil
		nmsgs[2].Key = nil
		_, err = l.Publish(nmsgs)
		require.NoError(t, err)

		deletedMsgs, cmp, err := CompactUpdates(context.TODO(), l, time.Now())
		require.NoError(t, err)
		require.Len(t, deletedMsgs, 1)
		require.Contains(t, deletedMsgs, nmsgs[1])
		require.Equal(t, l.Size(nmsgs[1]), cmp)
	})
}
