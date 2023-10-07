package trim

import (
	"context"
	"testing"

	"github.com/klev-dev/klevdb"
	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func TestByOffset(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, len(msgs), stat.Messages)

	msg, err := l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	t.Run("None", func(t *testing.T) {
		off, sz, err := ByOffset(context.TODO(), l, 0)
		require.Len(t, off, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), sz)

		stat, err = l.Stat()
		require.NoError(t, err)
		require.Equal(t, len(msgs), stat.Messages)

		msg, err = l.Get(klevdb.OffsetOldest)
		require.NoError(t, err)
		require.Equal(t, int64(0), msg.Offset)
	})

	t.Run("Half", func(t *testing.T) {
		off, sz, err := ByOffset(context.TODO(), l, 10)
		require.Len(t, off, 10)
		require.NoError(t, err)
		require.Equal(t, l.Size(msgs[0])*10, sz)

		stat, err = l.Stat()
		require.NoError(t, err)
		require.Equal(t, 10, stat.Messages)

		msg, err = l.Get(klevdb.OffsetOldest)
		require.NoError(t, err)
		require.Equal(t, int64(10), msg.Offset)
	})

	t.Run("All", func(t *testing.T) {
		off, sz, err := ByOffset(context.TODO(), l, 100)
		require.Len(t, off, 10)
		require.NoError(t, err)
		require.Equal(t, l.Size(msgs[0])*10, sz)

		stat, err = l.Stat()
		require.NoError(t, err)
		require.Equal(t, 0, stat.Messages)

		msg, err = l.Get(klevdb.OffsetOldest)
		require.ErrorIs(t, err, message.ErrInvalidOffset)
	})
}

func TestByOffsetRelative(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, len(msgs), stat.Messages)

	msg, err := l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	t.Run("Oldest", func(t *testing.T) {
		off, sz, err := ByOffset(context.TODO(), l, message.OffsetOldest)
		require.Len(t, off, 0)
		require.NoError(t, err)
		require.Equal(t, int64(0), sz)

		stat, err = l.Stat()
		require.NoError(t, err)
		require.Equal(t, len(msgs), stat.Messages)

		msg, err = l.Get(klevdb.OffsetOldest)
		require.NoError(t, err)
		require.Equal(t, int64(0), msg.Offset)
	})

	t.Run("Newest", func(t *testing.T) {
		off, sz, err := ByOffset(context.TODO(), l, message.OffsetNewest)
		require.Len(t, off, 20)
		require.NoError(t, err)
		require.Equal(t, l.Size(msgs[0])*20, sz)

		stat, err = l.Stat()
		require.NoError(t, err)
		require.Equal(t, 0, stat.Messages)

		msg, err = l.Get(klevdb.OffsetOldest)
		require.ErrorIs(t, err, message.ErrInvalidOffset)
	})
}
