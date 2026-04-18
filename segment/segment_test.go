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

func TestRecover(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
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
		corrupt func(s Segment) error
		out     []message.Message
	}{
		{
			"Ok",
			msgs,
			func(s Segment) error { return nil },
			msgs,
		},
		{
			"MessageMissing",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Data, message.Size(msgs[0], message.FormatLog))
			},
			msgs[0:1],
		},
		{
			"MessageShortHeader",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Data, message.Size(msgs[0], message.FormatLog)+4)
			},
			msgs[0:1],
		},
		{
			"MessageShortData",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Data, message.Size(msgs[0], message.FormatLog)+params.Size()+4)
			},
			msgs[0:1],
		},
		{
			"MessageCRC",
			msgs,
			func(s Segment) error {
				return clearLastByte(s.Data)
			},
			msgs[0:1],
		},
		{
			"IndexMissing",
			msgs,
			func(s Segment) error {
				return os.Remove(s.Index)
			},
			msgs,
		},
		{
			"IndexItemMissing",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Index, params.Size())
			},
			msgs,
		},
		{
			"IndexItemPartial",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Index, params.Size()+4)
			},
			msgs,
		},
		{
			"IndexItemWrong",
			msgs,
			func(s Segment) error {
				return clearLastByte(s.Index)
			},
			msgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seg := New(t.TempDir(), 0, message.FormatLog)
			writeMessages(t, seg, params, test.in)

			require.NoError(t, test.corrupt(seg))

			require.NoError(t, seg.Recover(params))

			assertMessages(t, seg, params, test.out)
		})
	}
}

func TestBackup(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
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
		backup func(t *testing.T, s Segment, dir string) error
		out    []message.Message
	}{
		{
			name: "Simple",
			in:   msgs,
			backup: func(t *testing.T, s Segment, dir string) error {
				return s.Backup(dir)
			},
			out: msgs,
		},
		{
			name: "Repeated",
			in:   msgs,
			backup: func(t *testing.T, s Segment, dir string) error {
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
			backup: func(t *testing.T, s Segment, dir string) error {
				if err := s.Backup(dir); err != nil {
					return err
				}

				writeMessages(t, s, params, msgs[1:])
				return s.Backup(dir)
			},
			out: msgs,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			seg := New(t.TempDir(), 0, message.FormatLog)
			writeMessages(t, seg, params, test.in)

			ndir := t.TempDir()
			require.NoError(t, test.backup(t, seg, ndir))

			nseg := New(ndir, 0, message.FormatLog)
			assertMessages(t, nseg, params, test.out)
		})
	}
}

func TestNeedsReindexHeaderOnly(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
	dir := t.TempDir()
	seg := New(dir, 0, message.FormatSegment)

	// Write just the header to the index (simulates crash after header write, before any items)
	iw, err := index.OpenWriter(seg.Index, params, index.FormatSegment)
	require.NoError(t, err)
	require.NoError(t, iw.Close())

	reindex, err := seg.NeedsReindex()
	require.NoError(t, err)
	require.True(t, reindex, "header-only index should trigger reindex")
}

func TestNeedsReindexPartialHeader(t *testing.T) {
	dir := t.TempDir()
	seg := New(dir, 0, message.FormatSegment)

	// Write a single byte to the index (simulates crash mid-header-write)
	f, err := os.Create(seg.Index)
	require.NoError(t, err)
	_, err = f.Write([]byte{0xFF})
	require.NoError(t, err)
	require.NoError(t, f.Close())

	reindex, err := seg.NeedsReindex()
	require.NoError(t, err)
	require.True(t, reindex, "partial-header index should trigger reindex")
}

func TestMigrate(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
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
			Value:  []byte("value1"),
		},
	}

	t.Run("Basic", func(t *testing.T) {
		dir := t.TempDir()
		oldSeg := New(dir, 0, message.FormatLog)
		writeMessages(t, oldSeg, params, msgs)

		newSeg, err := oldSeg.Migrate()
		require.NoError(t, err)
		require.Equal(t, message.FormatSegment, newSeg.DataFormat)

		// old .log file should be gone
		_, err = os.Stat(oldSeg.Data)
		require.ErrorIs(t, err, os.ErrNotExist)

		assertMessages(t, newSeg, params, msgs)
	})

	t.Run("NoOp", func(t *testing.T) {
		dir := t.TempDir()
		seg := New(dir, 0, message.FormatSegment)
		writeMessages(t, seg, params, msgs)

		result, err := seg.Migrate()
		require.NoError(t, err)
		require.Equal(t, seg, result)
	})

	t.Run("StaleTemp", func(t *testing.T) {
		dir := t.TempDir()
		oldSeg := New(dir, 0, message.FormatLog)
		writeMessages(t, oldSeg, params, msgs)

		// plant a stale .migrate temp file
		newSeg := New(dir, 0, message.FormatSegment)
		stale, err := os.Create(newSeg.Data + ".migrate")
		require.NoError(t, err)
		require.NoError(t, stale.Close())

		result, err := oldSeg.Migrate()
		require.NoError(t, err)
		require.Equal(t, message.FormatSegment, result.DataFormat)
		assertMessages(t, result, params, msgs)
	})
}

func writeMessages(t *testing.T, seg Segment, params index.Params, msgs []message.Message) {
	lw, err := message.OpenWriter(seg.Data, seg.DataFormat)
	require.NoError(t, err)
	iw, err := index.OpenWriter(seg.Index, params, seg.IndexFormat)
	require.NoError(t, err)

	var indexTime int64
	for _, msg := range msgs {
		pos, err := lw.Write(msg)
		require.NoError(t, err)
		item := params.NewItem(msg, pos, indexTime)
		err = iw.Write(item)
		require.NoError(t, err)
		indexTime = item.Timestamp
	}

	require.NoError(t, iw.Close())
	require.NoError(t, lw.Close())
}

func assertMessages(t *testing.T, seg Segment, params index.Params, expMsgs []message.Message) {
	index, err := seg.ReindexAndReadIndex(params)
	require.NoError(t, err)
	require.Len(t, index, len(expMsgs))

	lr, err := message.OpenReader(seg.Data, seg.DataFormat)
	require.NoError(t, err)

	for i, expMsg := range expMsgs {
		actIndex := index[i]

		require.Equal(t, expMsg.Offset, actIndex.Offset)
		require.Equal(t, expMsg.Time.UnixMicro(), actIndex.Timestamp)

		hasher := fnv.New64a()
		hasher.Write(expMsg.Key)
		require.Equal(t, hasher.Sum64(), actIndex.KeyHash)

		actMsg, err := lr.Get(actIndex.Position)
		require.NoError(t, err)

		require.Equal(t, expMsg.Offset, actMsg.Offset)
		require.Equal(t, expMsg.Time, actMsg.Time)
		require.Equal(t, expMsg.Key, actMsg.Key)
		require.Equal(t, expMsg.Value, actMsg.Value)
	}
}
