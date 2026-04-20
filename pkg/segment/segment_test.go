package segment

import (
	"hash/fnv"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/pkg/index"
	"github.com/klev-dev/klevdb/pkg/message"
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

	// V2 message size (with fixed overhead); truncation positions are header-relative.
	msg0Size := message.Size(msgs[0], message.V2)

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
				return os.Truncate(s.Log, message.HeaderSize+msg0Size)
			},
			msgs[0:1],
		},
		{
			"MessageShortHeader",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Log, message.HeaderSize+msg0Size+4)
			},
			msgs[0:1],
		},
		{
			"MessageShortData",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Log, message.HeaderSize+msg0Size+params.Size()+4)
			},
			msgs[0:1],
		},
		{
			"MessageCRC",
			msgs,
			func(s Segment) error {
				return clearLastByte(s.Log)
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
				// Truncate to header + fewer-than-one full item (triggers errIndexSize).
				return os.Truncate(s.Index, index.HeaderSize+params.Size()-1)
			},
			msgs,
		},
		{
			"IndexItemPartial",
			msgs,
			func(s Segment) error {
				return os.Truncate(s.Index, index.HeaderSize+params.Size()+4)
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
			seg := New(t.TempDir(), 0, false)
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
			seg := New(t.TempDir(), 0, false)
			writeMessages(t, seg, params, test.in)

			ndir := t.TempDir()
			require.NoError(t, test.backup(t, seg, ndir))

			nseg := New(ndir, 0, false)
			assertMessages(t, nseg, params, test.out)
		})
	}
}

func TestNeedsReindexHeaderOnly(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
	dir := t.TempDir()
	seg := New(dir, 0, false)

	// Write just the header to the index (simulates crash after header write, before any items)
	iw, err := index.OpenWriter(seg.Index, seg.Offset, index.V2, params)
	require.NoError(t, err)
	require.NoError(t, iw.Close())

	reindex, err := seg.NeedsReindex()
	require.NoError(t, err)
	require.True(t, reindex, "header-only index should trigger reindex")
}

func TestNeedsReindexPartialHeader(t *testing.T) {
	dir := t.TempDir()
	seg := New(dir, 0, false)

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
		seg := New(dir, 0, false)

		// Write a V1 log (no file header, same layout as the old pre-versioning format).
		// Migrate() is expected to rebuild the index from the log when none exists.
		lw, err := message.OpenWriter(seg.Log, seg.Offset, message.V1)
		require.NoError(t, err)
		for _, m := range msgs {
			_, err = lw.Write(m)
			require.NoError(t, err)
		}
		require.NoError(t, lw.SyncAndClose())

		err = seg.Migrate(message.V2, index.V2, params)
		require.NoError(t, err)

		// After migration the file should be readable as V2.
		r, err := message.OpenReader(seg.Log, seg.Offset)
		require.NoError(t, err)
		require.Equal(t, message.V2, r.Version())
		require.NoError(t, r.Close())

		assertMessages(t, seg, params, msgs)
	})

	t.Run("NoOp", func(t *testing.T) {
		dir := t.TempDir()
		seg := New(dir, 0, false)
		writeMessages(t, seg, params, msgs)

		err := seg.Migrate(message.V2, index.V2, params)
		require.NoError(t, err)
		assertMessages(t, seg, params, msgs)
	})

	t.Run("StaleTemp", func(t *testing.T) {
		dir := t.TempDir()
		seg := New(dir, 0, false)

		// Write a V1 log (no file header); Migrate() rebuilds the index from the log.
		lw, err := message.OpenWriter(seg.Log, seg.Offset, message.V1)
		require.NoError(t, err)
		for _, m := range msgs {
			_, err = lw.Write(m)
			require.NoError(t, err)
		}
		require.NoError(t, lw.SyncAndClose())

		// Plant a stale .migrate temp file.
		stale, err := os.Create(seg.Log + ".migrate")
		require.NoError(t, err)
		require.NoError(t, stale.Close())

		err = seg.Migrate(message.V2, index.V2, params)
		require.NoError(t, err)

		r, err := message.OpenReader(seg.Log, seg.Offset)
		require.NoError(t, err)
		require.Equal(t, message.V2, r.Version())
		require.NoError(t, r.Close())

		assertMessages(t, seg, params, msgs)
	})
}

func writeMessages(t *testing.T, seg Segment, params index.Params, msgs []message.Message) {
	lw, err := message.OpenWriter(seg.Log, seg.Offset, message.V2)
	require.NoError(t, err)
	iw, err := index.OpenWriter(seg.Index, seg.Offset, index.V2, params)
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
	idx, err := seg.ReindexAndReadIndex(params, index.V2)
	require.NoError(t, err)
	require.Len(t, idx, len(expMsgs))

	lr, err := message.OpenReader(seg.Log, seg.Offset)
	require.NoError(t, err)

	for i, expMsg := range expMsgs {
		actIndex := idx[i]

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
