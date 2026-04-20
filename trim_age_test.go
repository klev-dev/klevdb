package klevdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/pkg/message"
)

func TestTrimByAge(t *testing.T) {
	t.Run("Partial", testByAgePartial)
	t.Run("NoIndex", testByAgeNoIndex)
	t.Run("All", testByAgeAll)
}

func testByAgePartial(t *testing.T) {
	msgs := message.Gen(20)

	l, err := Open(t.TempDir(), Options{TimeIndex: true})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	msg, err := l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	trimTime := msgs[10].Time.Add(-time.Millisecond)
	_, trim, err := TrimByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, trim)

	msg, err = l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(10), msg.Offset)
}

func testByAgeNoIndex(t *testing.T) {
	msgs := message.Gen(20)

	l, err := Open(t.TempDir(), Options{})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	msg, err := l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(0), msg.Offset)

	trimTime := msgs[10].Time.Add(-time.Millisecond)
	_, trim, err := TrimByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Equal(t, l.Size(msgs[0])*10, trim)

	msg, err = l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, int64(10), msg.Offset)
}

func testByAgeAll(t *testing.T) {
	msgs := message.Gen(20)

	l, err := Open(t.TempDir(), Options{TimeIndex: true})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(msgs)
	require.NoError(t, err)

	trimTime := msgs[len(msgs)-1].Time.Add(time.Millisecond)
	off, sz, err := TrimByAge(context.TODO(), l, trimTime)
	require.NoError(t, err)
	require.Len(t, off, 20)
	require.Equal(t, l.Size(msgs[0])*20, sz)

	coff, cmsgs, err := l.Consume(OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(20), coff)
	require.Empty(t, cmsgs)
}
