package trim

import (
	"context"
	"testing"

	"github.com/klev-dev/klevdb"
	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func TestBySize(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*20, stat.Size)

	msg, err := l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	toTrimSize := l.Size(msgs[0]) * 11
	off, sz, err := BySize(context.TODO(), l, toTrimSize)
	require.Len(t, off, 10)
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, sz)

	msg, err = l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(10), msg.Offset)

	stat, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, stat.Size)
}
