package klevdb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/kleverr"
)

func publishBatched(t *testing.T, l Log, msgs []Message, batchLen int) {
	for begin := 0; begin < len(msgs); begin += batchLen {
		end := begin + batchLen
		if end > len(msgs) {
			end = len(msgs)
		}
		startOffset, err := l.NextOffset()
		require.NoError(t, err)

		nextOffset, err := l.Publish(msgs[begin:end])
		require.NoError(t, err)
		require.Equal(t, startOffset+int64(end-begin), nextOffset)
	}
}

func TestBasic(t *testing.T) {
	msgs := message.Gen(6)
	dir := t.TempDir()

	var l Log
	var err error
	t.Run("Open", func(t *testing.T) {
		l, err = Open(dir, Options{
			Rollover: 3 * message.Size(msgs[0]),
		})
		require.NoError(t, err)

		next, err := l.NextOffset()
		require.NoError(t, err)
		require.Equal(t, int64(0), next)

		stat, err := l.Stat()
		require.NoError(t, err)
		require.Equal(t, 1, stat.Segments)
		require.Equal(t, 0, stat.Messages)
		require.Equal(t, int64(0), stat.Size)
	})

	var coff int64
	var cmsgs []Message

	t.Run("EmptyRelative", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(OffsetOldest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(0), coff)
		require.Nil(t, cmsgs)

		coff, cmsgs, err = l.Consume(OffsetNewest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(0), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("EmptyAbsolute", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(0, 10)
		require.NoError(t, err)
		require.Equal(t, int64(0), coff)
		require.Nil(t, cmsgs)

		coff, cmsgs, err = l.Consume(1, 10)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, OffsetInvalid, coff)
		require.Nil(t, cmsgs)
	})

	t.Run("EmptyPublish", func(t *testing.T) {
		nextOffset, err := l.Publish(nil)
		require.NoError(t, err)
		require.Equal(t, int64(0), nextOffset)

		nextOffset, err = l.NextOffset()
		require.NoError(t, err)
		require.Equal(t, int64(0), nextOffset)
	})

	t.Run("Publish", func(t *testing.T) {
		for i := range msgs[0:3] {
			nextOffset, err := l.Publish(msgs[i : i+1])
			require.NoError(t, err)
			require.Equal(t, int64(i+1), nextOffset)

			nextOffset, err = l.NextOffset()
			require.NoError(t, err)
			require.Equal(t, int64(i+1), nextOffset)
		}

		nextOffset, err := l.NextOffset()
		require.NoError(t, err)
		require.Equal(t, int64(3), nextOffset)

		stat, err := l.Stat()
		require.NoError(t, err)
		require.Equal(t, 1, stat.Segments)
		require.Equal(t, 3, stat.Messages)
		require.Equal(t, 3*l.Size(msgs[0]), stat.Size)
	})

	t.Run("Relative", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(OffsetOldest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Equal(t, msgs[0:3], cmsgs)

		coff, cmsgs, err = l.Consume(OffsetNewest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Absolute", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(0, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Equal(t, msgs[0:3], cmsgs)

		coff, cmsgs, err = l.Consume(2, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Equal(t, msgs[2:3], cmsgs)

		coff, cmsgs, err = l.Consume(3, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Nil(t, cmsgs)

		coff, cmsgs, err = l.Consume(4, 10)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, OffsetInvalid, coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Rollover", func(t *testing.T) {
		nextOffset, err := l.Publish(msgs[3:])
		require.NoError(t, err)
		require.Equal(t, int64(len(msgs)), nextOffset)

		nextOffset, err = l.NextOffset()
		require.NoError(t, err)
		require.Equal(t, int64(len(msgs)), nextOffset)

		stat, err := l.Stat()
		require.NoError(t, err)
		require.Equal(t, 2, stat.Segments)
		require.Equal(t, 6, stat.Messages)
		require.Equal(t, 6*l.Size(msgs[0]), stat.Size)
	})

	t.Run("RolloverRelative", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(OffsetOldest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Equal(t, msgs[0:3], cmsgs)

		coff, cmsgs, err = l.Consume(OffsetNewest, 10)
		require.NoError(t, err)
		require.Equal(t, int64(6), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("RolloverAbsolute", func(t *testing.T) {
		coff, cmsgs, err = l.Consume(0, 3)
		require.NoError(t, err)
		require.Equal(t, int64(3), coff)
		require.Equal(t, cmsgs, msgs[0:3])

		coff, cmsgs, err = l.Consume(coff, 3)
		require.NoError(t, err)
		require.Equal(t, int64(6), coff)
		require.Equal(t, cmsgs, msgs[3:])

		coff, cmsgs, err = l.Consume(coff, 10)
		require.NoError(t, err)
		require.Equal(t, int64(6), coff)
		require.Nil(t, cmsgs)

		coff, cmsgs, err = l.Consume(coff+1, 10)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, OffsetInvalid, coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Close", func(t *testing.T) {
		err := l.Close()
		require.NoError(t, err)
	})
}

func TestGet(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		Rollover: 2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)
	defer l.Close()

	var gmsg Message

	t.Run("Empty", func(t *testing.T) {
		gmsg, err = l.Get(OffsetOldest)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)

		gmsg, err = l.Get(OffsetNewest)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)

		gmsg, err = l.Get(0)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)

		gmsg, err = l.Get(3)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)
	})

	publishBatched(t, l, msgs, 1)

	t.Run("Absolute", func(t *testing.T) {
		for _, msg := range msgs {
			gmsg, err = l.Get(msg.Offset)
			require.NoError(t, err)
			require.Equal(t, msg, gmsg)
		}
	})

	t.Run("Relative", func(t *testing.T) {
		gmsg, err = l.Get(OffsetInvalid)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)

		gmsg, err = l.Get(OffsetOldest)
		require.NoError(t, err)
		require.Equal(t, msgs[0], gmsg)

		gmsg, err = l.Get(OffsetNewest)
		require.NoError(t, err)
		require.Equal(t, msgs[len(msgs)-1], gmsg)
	})

	t.Run("Invalid", func(t *testing.T) {
		gmsg, err = l.Get(msgs[len(msgs)-1].Offset + 1)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)
	})
}

func TestByKey(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{})
		require.NoError(t, err)
		defer l.Close()

		gmsg, err := l.GetByKey([]byte("key"))
		require.ErrorIs(t, err, ErrNoIndex)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, err := l.OffsetByKey([]byte("key"))
		require.ErrorIs(t, err, ErrNoIndex)
		require.Equal(t, OffsetInvalid, ooff)

		coff, cmsgs, err := l.ConsumeByKey([]byte("key"), OffsetOldest, 32)
		require.ErrorIs(t, err, ErrNoIndex)
		require.Equal(t, OffsetInvalid, coff)
		require.Nil(t, cmsgs)
	})

	msgs := message.Gen(4)
	l, err := Open(t.TempDir(), Options{
		KeyIndex: true,
		Rollover: 2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)
	defer l.Close()

	t.Run("Empty", func(t *testing.T) {
		gmsg, err := l.GetByKey(msgs[0].Key)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, err := l.OffsetByKey(msgs[0].Key)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, OffsetInvalid, ooff)

		coff, cmsgs, err := l.ConsumeByKey(msgs[0].Key, OffsetOldest, 32)
		require.NoError(t, err)
		require.Equal(t, int64(0), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Put", func(t *testing.T) {
		publishBatched(t, l, msgs, 1)

		for i, msg := range msgs {
			gmsg, err := l.GetByKey(msg.Key)
			require.NoError(t, err)
			require.Equal(t, msg, gmsg)

			ooff, err := l.OffsetByKey(msg.Key)
			require.NoError(t, err)
			require.Equal(t, msg.Offset, ooff)

			coff, cmsgs, err := l.ConsumeByKey(msg.Key, OffsetOldest, 32)
			require.NoError(t, err)
			require.Equal(t, int64(i+1), coff)
			require.Len(t, cmsgs, 1)
			require.Equal(t, msg, cmsgs[0])

			// another search would return empty
			coff, cmsgs, err = l.ConsumeByKey(msg.Key, coff, 32)
			require.NoError(t, err)
			require.Equal(t, int64(4), coff)
			require.Nil(t, cmsgs)
		}
	})

	t.Run("After", func(t *testing.T) {
		coff, cmsgs, err := l.ConsumeByKey(msgs[0].Key, msgs[1].Offset, 32)
		require.NoError(t, err)
		require.Equal(t, int64(4), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Missing", func(t *testing.T) {
		gmsg, err := l.GetByKey([]byte("key"))
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, err := l.OffsetByKey([]byte("key"))
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, OffsetInvalid, ooff)

		coff, cmsgs, err := l.ConsumeByKey([]byte("key"), OffsetOldest, 32)
		require.NoError(t, err)
		require.Equal(t, int64(4), coff)
		require.Nil(t, cmsgs)
	})

	t.Run("Update", func(t *testing.T) {
		nmsgs := make([]Message, len(msgs))
		for i := range msgs {
			nmsgs[i].Time = msgs[i].Time.Add(time.Hour)
			nmsgs[i].Key = msgs[i].Key
			nmsgs[i].Value = append(msgs[i].Value, "world"...)
		}

		publishBatched(t, l, nmsgs, 1)

		for i := range msgs {
			gmsg, err := l.GetByKey(msgs[i].Key)
			require.NoError(t, err)
			require.Equal(t, nmsgs[i], gmsg)

			ooff, err := l.OffsetByKey(msgs[i].Key)
			require.NoError(t, err)
			require.Equal(t, nmsgs[i].Offset, ooff)

			coff, cmsgs, err := l.ConsumeByKey(msgs[i].Key, OffsetOldest, 32)
			require.NoError(t, err)
			require.Equal(t, int64(i+1), coff)
			require.Len(t, cmsgs, 1)
			require.Equal(t, msgs[i], cmsgs[0])

			coff, cmsgs, err = l.ConsumeByKey(msgs[i].Key, coff, 32)
			require.NoError(t, err)
			require.Equal(t, int64(len(msgs)+i+1), coff)
			require.Len(t, cmsgs, 1)
			require.Equal(t, nmsgs[i], cmsgs[0])
		}
	})
}

func TestByTime(t *testing.T) {
	t.Run("NoIndex", func(t *testing.T) {
		l, err := Open(t.TempDir(), Options{})
		require.NoError(t, err)
		defer l.Close()

		gmsg, err := l.GetByTime(time.Now())
		require.ErrorIs(t, err, ErrNoIndex)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, ots, err := l.OffsetByTime(time.Now())
		require.ErrorIs(t, err, ErrNoIndex)
		require.Equal(t, OffsetInvalid, ooff)
		require.Zero(t, ots)
	})

	msgs := message.Gen(4)
	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)
	defer l.Close()

	t.Run("Empty", func(t *testing.T) {
		gmsg, err := l.GetByTime(msgs[0].Time)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, ots, err := l.OffsetByTime(msgs[0].Time)
		require.ErrorIs(t, err, ErrInvalidOffset)
		require.Equal(t, OffsetInvalid, ooff)
		require.Zero(t, ots)
	})

	t.Run("Absolute", func(t *testing.T) {
		publishBatched(t, l, msgs, 1)

		for _, msg := range msgs {
			gmsg, err := l.GetByTime(msg.Time)
			require.NoError(t, err)
			require.Equal(t, msg, gmsg)

			ooff, ots, err := l.OffsetByTime(msg.Time)
			require.NoError(t, err)
			require.Equal(t, msg.Offset, ooff)
			require.Equal(t, msg.Time, ots)
		}
	})

	t.Run("Before", func(t *testing.T) {
		before := msgs[0].Time.Add(-time.Hour)

		gmsg, err := l.GetByTime(before)
		require.NoError(t, err)
		require.Equal(t, msgs[0], gmsg)

		ooff, ots, err := l.OffsetByTime(before)
		require.NoError(t, err)
		require.Equal(t, msgs[0].Offset, ooff)
		require.Equal(t, msgs[0].Time, ots)
	})

	t.Run("After", func(t *testing.T) {
		before := msgs[len(msgs)-1].Time.Add(time.Hour)

		gmsg, err := l.GetByTime(before)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, InvalidMessage, gmsg)

		ooff, ots, err := l.OffsetByTime(before)
		require.ErrorIs(t, err, ErrNotFound)
		require.Equal(t, OffsetInvalid, ooff)
		require.Zero(t, ots)
	})

	t.Run("Mid", func(t *testing.T) {
		before := msgs[2].Time.Add(-time.Microsecond)

		gmsg, err := l.GetByTime(before)
		require.NoError(t, err)
		require.Equal(t, msgs[2], gmsg)

		ooff, ots, err := l.OffsetByTime(before)
		require.NoError(t, err)
		require.Equal(t, msgs[2].Offset, ooff)
		require.Equal(t, msgs[2].Time, ots)
	})
}

func TestByTimeMono(t *testing.T) {
	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
	})
	require.NoError(t, err)
	defer l.Close()

	msgs := message.Gen(5)
	msgs[1], msgs[3] = msgs[3], msgs[1]
	publishBatched(t, l, msgs, 1)

	gmsg, err := l.GetByTime(msgs[1].Time)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)

	gmsg, err = l.GetByTime(msgs[2].Time)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)

	gmsg, err = l.GetByTime(msgs[3].Time)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)
}

func TestReopen(t *testing.T) {
	t.Run("Segment", testReopenSegment)
	t.Run("Segments", testReopenSegments)
}

func testReopenSegment(t *testing.T) {
	msgs := message.Gen(4)
	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)

	publishBatched(t, l, msgs, 1)

	require.NoError(t, l.Close())

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	coff, cmsgs, err := l.Consume(OffsetOldest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(len(msgs)), coff)
	require.Equal(t, msgs, cmsgs)

	for i, msg := range msgs {
		gmsg, err := l.GetByKey(msgs[i].Key)
		require.NoError(t, err)
		require.Equal(t, msg, gmsg)
	}
}

func testReopenSegments(t *testing.T) {
	msgs := message.Gen(4)
	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)

	publishBatched(t, l, msgs, 1)

	require.NoError(t, l.Close())

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	coff, cmsgs, err := l.Consume(OffsetOldest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(2), coff)
	require.Equal(t, msgs[0:2], cmsgs)

	coff, cmsgs, err = l.Consume(coff, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Equal(t, msgs[2:], cmsgs)

	for i, msg := range msgs {
		gmsg, err := l.GetByKey(msgs[i].Key)
		require.NoError(t, err)
		require.Equal(t, msg, gmsg)
	}
}

func TestReadonly(t *testing.T) {
	t.Run("Empty", testReadonlyEmpty)
	t.Run("Segment", testReadonlySegment)
	t.Run("Segments", testReadonlySegments)
}

func testReadonlyEmpty(t *testing.T) {
	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Readonly:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(nil)
	require.ErrorIs(t, err, ErrReadonly)

	noff, err := l.NextOffset()
	require.NoError(t, err)
	require.Equal(t, int64(0), noff)

	_, _, err = l.Delete(nil)
	require.ErrorIs(t, err, ErrReadonly)

	// Consume checks
	coff, cmsgs, err := l.Consume(OffsetOldest, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(OffsetNewest, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(0), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(1, 1)
	require.ErrorIs(t, err, ErrInvalidOffset)
	require.Equal(t, OffsetInvalid, coff)
	require.Empty(t, cmsgs)

	// Get checks
	_, err = l.Get(OffsetOldest)
	require.ErrorIs(t, err, ErrInvalidOffset)

	_, err = l.Get(OffsetNewest)
	require.ErrorIs(t, err, ErrInvalidOffset)

	_, err = l.Get(0)
	require.ErrorIs(t, err, ErrInvalidOffset)

	_, err = l.Get(1)
	require.ErrorIs(t, err, ErrInvalidOffset)

	// Other getters checks
	_, err = l.GetByKey([]byte("abc"))
	require.ErrorIs(t, err, ErrNotFound)

	_, err = l.GetByTime(time.Now().UTC())
	require.ErrorIs(t, err, ErrInvalidOffset)

	// Others
	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 0, stat.Segments)
	require.Equal(t, 0, stat.Messages)
	require.Equal(t, int64(0), stat.Size)

	err = l.Backup(t.TempDir())
	require.NoError(t, err)

	err = l.Sync()
	require.NoError(t, err)

	err = l.Close()
	require.NoError(t, err)
}

func testReadonlySegment(t *testing.T) {
	msgs := message.Gen(4)
	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)

	publishBatched(t, l, msgs, 1)

	err = l.Close()
	require.NoError(t, err)

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Readonly:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(nil)
	require.ErrorIs(t, err, ErrReadonly)

	noff, err := l.NextOffset()
	require.NoError(t, err)
	require.Equal(t, int64(4), noff)

	_, _, err = l.Delete(nil)
	require.ErrorIs(t, err, ErrReadonly)

	// Consume checks
	coff, cmsgs, err := l.Consume(OffsetOldest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Equal(t, msgs, cmsgs)

	coff, cmsgs, err = l.Consume(OffsetNewest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(0, 1)
	require.NoError(t, err)
	require.Equal(t, int64(1), coff)
	require.Equal(t, msgs[0:1], cmsgs)

	coff, cmsgs, err = l.Consume(4, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(5, 1)
	require.ErrorIs(t, err, ErrInvalidOffset)
	require.Equal(t, OffsetInvalid, coff)
	require.Empty(t, cmsgs)

	// Get checks
	gmsg, err := l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, msgs[0], gmsg)

	gmsg, err = l.Get(OffsetNewest)
	require.NoError(t, err)
	require.Equal(t, msgs[3], gmsg)

	gmsg, err = l.Get(2)
	require.NoError(t, err)
	require.Equal(t, msgs[2], gmsg)

	_, err = l.Get(5)
	require.ErrorIs(t, err, ErrInvalidOffset)

	// Other getters checks
	gmsg, err = l.GetByKey(msgs[1].Key)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)

	gmsg, err = l.GetByTime(msgs[2].Time)
	require.NoError(t, err)
	require.Equal(t, msgs[2], gmsg)

	// Others
	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 1, stat.Segments)
	require.Equal(t, 4, stat.Messages)
	require.Equal(t, 4*l.Size(msgs[0]), stat.Size)

	bdir := t.TempDir()
	err = l.Backup(bdir)
	require.NoError(t, err)

	bstat, err := Stat(bdir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(t, err)
	require.Equal(t, stat, bstat)

	err = l.Sync()
	require.NoError(t, err)

	err = l.Close()
	require.NoError(t, err)
}

func testReadonlySegments(t *testing.T) {
	msgs := message.Gen(4)
	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)

	publishBatched(t, l, msgs, 1)

	err = l.Close()
	require.NoError(t, err)

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Readonly:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	_, err = l.Publish(nil)
	require.ErrorIs(t, err, ErrReadonly)

	noff, err := l.NextOffset()
	require.NoError(t, err)
	require.Equal(t, int64(4), noff)

	_, _, err = l.Delete(nil)
	require.ErrorIs(t, err, ErrReadonly)

	// Consume checks
	coff, cmsgs, err := l.Consume(OffsetOldest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(2), coff)
	require.Equal(t, msgs[0:2], cmsgs)

	coff, cmsgs, err = l.Consume(OffsetNewest, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(2, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Equal(t, msgs[2:], cmsgs)

	coff, cmsgs, err = l.Consume(4, 4)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(5, 1)
	require.ErrorIs(t, err, ErrInvalidOffset)
	require.Equal(t, OffsetInvalid, coff)
	require.Empty(t, cmsgs)

	// Get checks
	gmsg, err := l.Get(OffsetOldest)
	require.NoError(t, err)
	require.Equal(t, msgs[0], gmsg)

	gmsg, err = l.Get(OffsetNewest)
	require.NoError(t, err)
	require.Equal(t, msgs[3], gmsg)

	gmsg, err = l.Get(2)
	require.NoError(t, err)
	require.Equal(t, msgs[2], gmsg)

	_, err = l.Get(5)
	require.ErrorIs(t, err, ErrInvalidOffset)

	// Other getters checks
	gmsg, err = l.GetByKey(msgs[1].Key)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)

	gmsg, err = l.GetByTime(msgs[2].Time)
	require.NoError(t, err)
	require.Equal(t, msgs[2], gmsg)

	// Others
	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stat.Segments)
	require.Equal(t, 4, stat.Messages)
	require.Equal(t, 4*l.Size(msgs[0]), stat.Size)

	bdir := t.TempDir()
	err = l.Backup(bdir)
	require.NoError(t, err)

	bstat, err := Stat(bdir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(t, err)
	require.Equal(t, stat, bstat)

	err = l.Sync()
	require.NoError(t, err)

	err = l.Close()
	require.NoError(t, err)
}

func TestStat(t *testing.T) {
	t.Run("Segment", testStatSegment)
	t.Run("Segments", testStatSegments)
}

func testStatSegment(t *testing.T) {
	msgs := message.Gen(3)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	sz := int64(0)
	for _, msg := range msgs {
		sz += l.Size(msg)
	}

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 1, stats.Segments)
	require.Equal(t, len(msgs), stats.Messages)
	require.Equal(t, sz, stats.Size)
}

func testStatSegments(t *testing.T) {
	msgs := message.Gen(3)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	sz := int64(0)
	for _, msg := range msgs {
		sz += l.Size(msg)
	}

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stats.Segments)
	require.Equal(t, len(msgs), stats.Messages)
	require.Equal(t, sz, stats.Size)
}

func TestBackup(t *testing.T) {
	t.Run("Segment", testBackupSegment)
	t.Run("Segments", testBackupSegments)
}

func testBackupSegment(t *testing.T) {
	msgs := message.Gen(3)

	l, err := Open(t.TempDir(), Options{TimeIndex: true, KeyIndex: true})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 1, stat.Segments)

	bdir := t.TempDir()
	l.Backup(bdir)

	bl, err := Open(bdir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(t, err)
	defer bl.Close()

	startOffset := OffsetOldest
	for {
		coff, cmsgs, err := l.Consume(startOffset, 1)
		require.NoError(t, err)

		boff, bmsgs, err := bl.Consume(startOffset, 1)
		require.NoError(t, err)

		require.Equal(t, coff, boff)
		require.Equal(t, cmsgs, bmsgs)

		if startOffset == coff {
			break
		}
		startOffset = coff
	}
}

func testBackupSegments(t *testing.T) {
	msgs := message.Gen(3)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stat, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stat.Segments)

	bdir := t.TempDir()
	l.Backup(bdir)

	bl, err := Open(bdir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(t, err)
	defer bl.Close()

	startOffset := OffsetOldest
	for {
		coff, cmsgs, err := l.Consume(startOffset, 1)
		require.NoError(t, err)

		boff, bmsgs, err := bl.Consume(startOffset, 1)
		require.NoError(t, err)

		require.Equal(t, coff, boff)
		require.Equal(t, cmsgs, bmsgs)

		if startOffset == coff {
			break
		}
		startOffset = coff
	}
}

func TestReindex(t *testing.T) {
	msgs := message.Gen(4)

	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)

	publishBatched(t, l, msgs, 1)
	require.NoError(t, l.Close())

	// delete all index files
	files, err := os.ReadDir(dir)
	require.NoError(t, err)
	for _, f := range files {
		if strings.HasSuffix(f.Name(), ".index") {
			name := filepath.Join(dir, f.Name())
			err := os.Remove(name)
			require.NoError(t, err)
		}
	}

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * message.Size(msgs[0]),
	})
	require.NoError(t, err)

	coff, cmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(2), coff)
	require.Equal(t, msgs[0:2], cmsgs)

	coff, cmsgs, err = l.Consume(coff, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Equal(t, msgs[2:], cmsgs)

	gmsg, err := l.GetByKey(msgs[1].Key)
	require.NoError(t, err)
	require.Equal(t, msgs[1], gmsg)
}

func TestDelete(t *testing.T) {
	t.Run("ReaderPartial", testDeleteReaderPartial)
	t.Run("ReaderPartialReload", testDeleteReaderPartialReload)
	t.Run("ReaderFull", testDeleteReaderFull)
	t.Run("WriterSingle", testDeleteWriterSingle)
	t.Run("WriterLast", testDeleteWriterLast)
	t.Run("WriterPartial", testDeleteWriterPartial)
	t.Run("WriterFull", testDeleteWriterFull)
	t.Run("All", testDeleteAll)
}

func testDeleteReaderPartial(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		0: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 1)
	require.Contains(t, offsets, int64(0))
	require.Equal(t, l.Size(msgs[0]), sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 3, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(2), doff)
	require.Equal(t, msgs[1], dmsgs[0])

	_, err = l.Get(0)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = l.GetByKey(msgs[0].Key)
	require.ErrorIs(t, err, ErrNotFound)
}

func testDeleteReaderPartialReload(t *testing.T) {
	dir := t.TempDir()
	msgs := message.Gen(4)

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	publishBatched(t, l, msgs, 1)

	err = l.Close()
	require.NoError(t, err)

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		0: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 1)
	require.Contains(t, offsets, int64(0))
	require.Equal(t, l.Size(msgs[0]), sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 3, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(2), doff)
	require.Equal(t, msgs[1], dmsgs[0])

	_, err = l.Get(0)
	require.ErrorIs(t, err, ErrNotFound)

	_, err = l.GetByKey(msgs[0].Key)
	require.ErrorIs(t, err, ErrNotFound)
}

func testDeleteReaderFull(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		0: {},
		1: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Contains(t, offsets, int64(0))
	require.Contains(t, offsets, int64(1))
	require.Equal(t, l.Size(msgs[0])*2, sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), doff)
	require.Equal(t, msgs[2:], dmsgs)
}

func testDeleteWriterSingle(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  4 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 1, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		0: {},
		1: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Contains(t, offsets, int64(0))
	require.Contains(t, offsets, int64(1))
	require.Equal(t, l.Size(msgs[0])*2, sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 1, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), doff)
	require.Equal(t, msgs[2:4], dmsgs)

	doff, dmsgs, err = l.Consume(1, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), doff)
	require.Equal(t, msgs[2:4], dmsgs)
}

func testDeleteWriterLast(t *testing.T) {
	msgs := message.Gen(4)
	dir := t.TempDir()

	l, err := Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)
	nextOffset, err := l.NextOffset()
	require.NoError(t, err)
	require.Equal(t, int64(4), nextOffset)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		3: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 1)
	require.Contains(t, offsets, int64(3))
	require.Equal(t, l.Size(msgs[0]), sz)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 3, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	err = l.Close()
	require.NoError(t, err)

	l, err = Open(dir, Options{
		TimeIndex: true,
		KeyIndex:  true,
	})
	require.NoError(t, err)
	defer l.Close()

	nextOffset, err = l.NextOffset()
	require.NoError(t, err)
	require.Equal(t, int64(4), nextOffset)
}

func testDeleteWriterPartial(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		2: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 1)
	require.Contains(t, offsets, int64(2))
	require.Equal(t, l.Size(msgs[0]), sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 3, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(2), doff)
	require.Equal(t, msgs[0:2], dmsgs)

	doff, dmsgs, err = l.Consume(2, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), doff)
	require.Equal(t, msgs[3:], dmsgs)

	_, err = l.Get(2)
	require.ErrorIs(t, err, ErrNotFound)
}

func testDeleteWriterFull(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 4, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	offsets, sz, err := l.Delete(map[int64]struct{}{
		2: {},
		3: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Contains(t, offsets, int64(2))
	require.Contains(t, offsets, int64(3))
	require.Equal(t, l.Size(msgs[0])*2, sz)

	stats, err = l.Stat()
	require.NoError(t, err)
	require.Equal(t, 2, stats.Messages)
	require.Equal(t, 2, stats.Segments)

	doff, dmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(2), doff)
	require.Equal(t, msgs[0:2], dmsgs)

	doff, dmsgs, err = l.Consume(2, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), doff)
	require.Empty(t, dmsgs)

	poff, err := l.Publish(msgs[2:])
	require.NoError(t, err)
	require.Equal(t, int64(6), poff)

	doff, dmsgs, err = l.Consume(2, 32)
	require.NoError(t, err)
	require.Equal(t, int64(6), doff)
	require.Equal(t, msgs[2:], dmsgs)
}

func testDeleteAll(t *testing.T) {
	msgs := message.Gen(4)

	l, err := Open(t.TempDir(), Options{
		TimeIndex: true,
		KeyIndex:  true,
		Rollover:  2 * (message.Size(msgs[0]) - 1),
	})
	require.NoError(t, err)
	defer l.Close()

	publishBatched(t, l, msgs, 1)

	// delete the writer segment
	offsets, sz, err := l.Delete(map[int64]struct{}{
		2: {},
		3: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Contains(t, offsets, int64(2))
	require.Contains(t, offsets, int64(3))
	require.Equal(t, l.Size(msgs[0])*2, sz)

	// delete the reader segment
	offsets, sz, err = l.Delete(map[int64]struct{}{
		0: {},
		1: {},
	})
	require.NoError(t, err)
	require.Len(t, offsets, 2)
	require.Contains(t, offsets, int64(0))
	require.Contains(t, offsets, int64(1))
	require.Equal(t, l.Size(msgs[0])*2, sz)

	stats, err := l.Stat()
	require.NoError(t, err)
	require.Equal(t, 0, stats.Messages)
	require.Equal(t, 1, stats.Segments)

	coff, cmsgs, err := l.Consume(message.OffsetOldest, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	coff, cmsgs, err = l.Consume(1, 32)
	require.NoError(t, err)
	require.Equal(t, int64(4), coff)
	require.Empty(t, cmsgs)

	poff, err := l.Publish(msgs[0:1])
	require.NoError(t, err)
	require.Equal(t, int64(len(msgs)+1), poff)
}

func TestConcurrent(t *testing.T) {
	t.Run("PubsubRecent", testConcurrentPubsubRecent)
	t.Run("Consume", testConcurrentConsume)
	t.Run("Delete", testConcurrentDelete)
}

func testConcurrentPubsubRecent(t *testing.T) {
	defer os.RemoveAll("test_pubsub")
	s, err := Open("test_pubsub", Options{
		CreateDirs: true,
		AutoSync:   true,
		Check:      true,
		Rollover:   1024 * 64,
	})
	require.NoError(t, err)
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	g, ctx := errgroup.WithContext(ctx)

	g.Go(func() error {
		for i := 0; ctx.Err() == nil; i++ {
			msgs := []Message{{
				Key: []byte(fmt.Sprintf("%010d", i)),
			}}
			_, err := s.Publish(msgs)
			if err != nil {
				return err
			}
		}
		return nil
	})

	g.Go(func() error {
		var offset = OffsetOldest
		for ctx.Err() == nil {
			next, msgs, err := s.Consume(offset, 32)
			if err != nil {
				return kleverr.Newf("could not consume offset %d: %w", offset, err)
			}

			if offset == next {
				offset = offset - 16
				continue
			}

			offset = next
			for _, msg := range msgs {
				require.Equal(t, []byte(fmt.Sprintf("%010d", msg.Offset)), msg.Key)
			}
		}
		return nil
	})

	// g.Go(func() error {
	// 	for ctx.Err() == nil {
	// 		_, msgs, err := s.Consume(OffsetOldest, 32)
	// 		if err != nil {
	// 			return err
	// 		}

	// 		var del = make(map[int64]struct{}, len(msgs))
	// 		for _, msg := range msgs {
	// 			del[msg.Offset] = struct{}{}
	// 		}
	// 		_, _, err = s.Delete(del)
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// })

	err = g.Wait()
	if serr := kleverr.Get(err); serr != nil {
		fmt.Println(serr.Print())
	}
	require.NoError(t, err)
}

func testConcurrentConsume(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true})
	require.NoError(t, err)
	defer s.Close()

	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for i := 0; i < 10000; i++ {
				msgs := []Message{{
					Key: []byte(fmt.Sprintf("%02d", i)),
				}}
				_, err := s.Publish(msgs)
				require.NoError(t, err)
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			offset := OffsetOldest
			for offset < 30000 {
				next, _, err := s.Consume(offset, 1)
				require.NoError(t, err)
				offset = next
			}
		}()

		wg.Add(1)
		go func() {
			defer wg.Done()

			time.Sleep(time.Millisecond)
			for i := 0; i < 10000; i++ {
				k := []byte(fmt.Sprintf("%02d", i))
				_, err := s.GetByKey(k)
				if errors.Is(err, ErrNotFound) {
					i--
					continue
				}
				require.NoError(t, err, "key %s", k)
			}
		}()
	}

	wg.Wait()
}

func testConcurrentDelete(t *testing.T) {
	dir := t.TempDir()

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true})
	require.NoError(t, err)
	defer s.Close()

	msgs := message.Gen(10000)
	msgSize := s.Size(msgs[0])

	var wg sync.WaitGroup
	wg.Add(1)
	wg.Add(1)

	go func() {
		defer wg.Done()

		for i := 0; i < len(msgs); i += 10 {
			_, err := s.Publish(msgs[i : i+10])
			require.NoError(t, err)
		}
	}()

	go func() {
		defer wg.Done()

		for !t.Failed() {
			next, msgs, err := s.Consume(OffsetOldest, 10)
			require.NoError(t, err)
			offsets := make(map[int64]struct{})
			for _, msg := range msgs {
				offsets[msg.Offset] = struct{}{}
			}
			deleted, sz, err := s.Delete(offsets)
			require.NoError(t, err)
			require.Len(t, deleted, len(offsets))
			require.Equal(t, int64(len(msgs))*msgSize, sz)
			if next >= int64(len(msgs)) {
				break
			}
			if len(msgs) == 0 {
				time.Sleep(time.Millisecond)
			}
		}
	}()

	wg.Wait()
}
