package trim

import (
	"context"
	"testing"
	"time"

	"github.com/klev-dev/klevdb"
	"github.com/klev-dev/klevdb/message"
	"github.com/stretchr/testify/require"
)

func TestTrim(t *testing.T) {
	t.Run("BySize", testBySize)
	t.Run("ByAge", testByAge)
	t.Run("ByAgeNoIndex", testByAgeNoIndex)
	t.Run("ByAgeAll", testByAgeAll)
}

func testBySize(t *testing.T) {
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

func testByAge(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{TimeIndex: true})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	msg, err := l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	trimTime := msgs[10].Time.Add(-time.Millisecond)
	_, trim, err := ByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, trim)

	msg, err = l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(10), msg.Offset)
}

func testByAgeNoIndex(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	msg, err := l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	trimTime := msgs[10].Time.Add(-time.Millisecond)
	_, trim, err := ByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, trim)

	msg, err = l.Get(klevdb.OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(10), msg.Offset)
}

func testByAgeAll(t *testing.T) {
	msgs := message.Gen(20)

	l, err := klevdb.Open(t.TempDir(), klevdb.Options{TimeIndex: true})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	trimTime := msgs[len(msgs)-1].Time.Add(time.Millisecond)
	off, sz, err := ByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Len(t, off, 20)
	require.Equal(t, l.Size(msgs[0])*20, sz)

	coff, cmsgs, err := l.Consume(klevdb.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(20), coff)
	require.Empty(t, cmsgs)
}
