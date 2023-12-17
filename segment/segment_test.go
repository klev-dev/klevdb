package segment

import (
	"hash/fnv"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
)

func clearLastByte(fn string) error {
	f, err := os.OpenFile(fn, os.O_RDWR, 0600)
	if err != nil {
		return err
	}

	_, err = f.Seek(-1, 2)
	if err != nil {
		return err
	}

	_, err = f.Write([]byte{0})
	if err != nil {
		return err
	}

	return f.Close()
}

type timeKeyIndexSegment = Segment[
	index.TimeKeyIndex,
	index.TimeKeyItem,
	int64,
	*index.TimeKeyIndexRuntime,
]

func TestRecover(t *testing.T) {
	ix := index.TimeKeyIndex{}
	msgs := []message.Message{
		{
			Offset: 0,
			Time:   time.Date(2022, 04, 03, 14, 58, 0, 0, time.UTC),
			Key:    []byte("key"),
			Value:  []byte("value"),
		},
		{
			Offset: 1,
			Time:   time.Date(2022, 04, 03, 15, 58, 0, 0, time.UTC),
			Key:    []byte("key1"),
			Value:  []byte("value"),
		},
	}

	var tests = []struct {
		name    string
		in      []message.Message
		corrupt func(s timeKeyIndexSegment) error
		out     []message.Message
	}{
		{
			"Ok",
			msgs,
			func(s timeKeyIndexSegment) error {
				return nil
			},
			msgs,
		},
		{
			"MessageMissing",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Truncate(s.Log, message.Size(msgs[0]))
			},
			msgs[0:1],
		},
		{
			"MessageShortHeader",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Truncate(s.Log, message.Size(msgs[0])+4)
			},
			msgs[0:1],
		},
		{
			"MessageShortData",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Truncate(s.Log, message.Size(msgs[0])+ix.Size()+4)
			},
			msgs[0:1],
		},
		{
			"MessageCRC",
			msgs,
			func(s timeKeyIndexSegment) error {
				return clearLastByte(s.Log)
			},
			msgs[0:1],
		},
		{
			"IndexMissing",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Remove(s.Index)
			},
			msgs,
		},
		{
			"IndexItemMissing",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Truncate(s.Index, ix.Size())
			},
			msgs,
		},
		{
			"IndexItemPartial",
			msgs,
			func(s timeKeyIndexSegment) error {
				return os.Truncate(s.Index, ix.Size()+4)
			},
			msgs,
		},
		{
			"IndexItemWrong",
			msgs,
			func(s timeKeyIndexSegment) error {
				return clearLastByte(s.Index)
			},
			msgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seg := New[index.TimeKeyIndex, index.TimeKeyItem, int64, *index.TimeKeyIndexRuntime](t.TempDir(), 0)
			writeMessages(t, seg, ix, test.in)

			require.NoError(t, test.corrupt(seg))

			require.NoError(t, seg.Recover(ix))

			assertMessages(t, seg, ix, test.out)
		})
	}
}

func TestBackup(t *testing.T) {
	ix := index.TimeKeyIndex{}
	msgs := []message.Message{
		{
			Offset: 0,
			Time:   time.Date(2022, 04, 03, 14, 58, 0, 0, time.UTC),
			Key:    []byte("key"),
			Value:  []byte("value"),
		},
		{
			Offset: 1,
			Time:   time.Date(2022, 04, 03, 15, 58, 0, 0, time.UTC),
			Key:    []byte("key1"),
			Value:  []byte("value"),
		},
	}

	var tests = []struct {
		name   string
		in     []message.Message
		backup func(t *testing.T, s timeKeyIndexSegment, dir string) error
		out    []message.Message
	}{
		{
			name: "Simple",
			in:   msgs,
			backup: func(t *testing.T, s timeKeyIndexSegment, dir string) error {
				return s.Backup(dir)
			},
			out: msgs,
		},
		{
			name: "Repeated",
			in:   msgs,
			backup: func(t *testing.T, s timeKeyIndexSegment, dir string) error {
				if err := s.Backup(dir); err != nil {
					return err
				}
				return s.Backup(dir)
			},
			out: msgs,
		},
		{
			name: "Incremental",
			in:   msgs[0:1],
			backup: func(t *testing.T, s timeKeyIndexSegment, dir string) error {
				if err := s.Backup(dir); err != nil {
					return err
				}

				writeMessages(t, s, ix, msgs[1:])
				return s.Backup(dir)
			},
			out: msgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seg := New[index.TimeKeyIndex](t.TempDir(), 0)
			writeMessages(t, seg, ix, test.in)

			ndir := t.TempDir()
			require.NoError(t, test.backup(t, seg, ndir))

			nseg := New[index.TimeKeyIndex](ndir, 0)
			assertMessages(t, nseg, ix, test.out)
		})
	}
}

func writeMessages[IX index.Index[IT, IC, IS], IT index.Item, IC index.State, IS index.Runtime](t *testing.T, seg Segment[IX, IT, IC, IS], ix IX, msgs []message.Message) {
	lw, err := message.OpenWriter(seg.Log)
	require.NoError(t, err)
	iw, err := index.OpenWriter(seg.Index, ix)
	require.NoError(t, err)

	var state = ix.NewState()
	for _, msg := range msgs {
		pos, err := lw.Write(msg)
		require.NoError(t, err)

		item, nextState, err := ix.New(msg, pos, state)
		require.NoError(t, err)

		err = iw.Write(item)
		require.NoError(t, err)

		state = nextState
	}

	require.NoError(t, iw.Close())
	require.NoError(t, lw.Close())
}

func assertMessages(t *testing.T, seg timeKeyIndexSegment, ix index.TimeKeyIndex, expMsgs []message.Message) {
	index, err := seg.ReindexAndReadIndex(ix)
	require.NoError(t, err)
	require.Len(t, index, len(expMsgs))

	lr, err := message.OpenReader(seg.Log)
	require.NoError(t, err)

	for i, expMsg := range expMsgs {
		actIndex := index[i]

		require.Equal(t, expMsg.Offset, actIndex.Offset())
		require.Equal(t, expMsg.Time.UnixMicro(), actIndex.Timestamp())

		hasher := fnv.New64a()
		hasher.Write(expMsg.Key)
		require.Equal(t, hasher.Sum64(), actIndex.KeyHash())

		actMsg, err := lr.Get(actIndex.Position())
		require.NoError(t, err)

		require.Equal(t, expMsg.Offset, actMsg.Offset)
		require.Equal(t, expMsg.Time, actMsg.Time)
		require.Equal(t, expMsg.Key, actMsg.Key)
		require.Equal(t, expMsg.Value, actMsg.Value)
	}
}
