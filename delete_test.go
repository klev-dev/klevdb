package klevdb

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/pkg/message"
)

func TestDeleteMulti(t *testing.T) {
	msgs := message.Gen(10)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0], message.V2) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 10, stats.Messages)
	require.Equal(t, 5, stats.Segments)

	msgs, sz, err := DeleteMulti(context.TODO(), l, map[int64]struct{}{
		0: {},
		2: {},
		3: {},
		4: {},
	}, DeleteMultiWithWait(time.Millisecond))
	require.NoError(t, err)
	require.Len(t, msgs, 4)
	offsets := getOffsets(msgs)
	require.Contains(t, offsets, int64(0))
	require.Contains(t, offsets, int64(2))
	require.Contains(t, offsets, int64(3))
	require.Contains(t, offsets, int64(4))
	require.Equal(t, l.Size(msgs[0])*4, sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 6, stats.Messages)
	require.Equal(t, 4, stats.Segments)
}
