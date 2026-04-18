package segment

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
)

func TestFindPartialMigration(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
	msgs := []message.Message{
		{
			Offset: 0,
			Time:   time.Date(2022, 04, 03, 14, 58, 0, 0, time.UTC),
			Key:    []byte("key"),
			Value:  []byte("value"),
		},
	}

	dir := t.TempDir()

	// Create both .log and .segment for offset 0 (simulates partial migration).
	// Delete the index between writes: Migrate() removes the old index before creating
	// the new segment, so they never share an index written in the wrong format.
	oldSeg := New(dir, 0, message.FormatLog)
	writeMessages(t, oldSeg, params, msgs)
	require.NoError(t, os.Remove(oldSeg.Index))
	newSeg := New(dir, 0, message.FormatSegment)
	writeMessages(t, newSeg, params, msgs)

	segments, err := Find(dir)
	require.NoError(t, err)
	require.Len(t, segments, 1)
	require.Equal(t, message.FormatSegment, segments[0].DataFormat, ".segment should win over .log")
}

func TestCleanupOrphanLogs(t *testing.T) {
	params := index.Params{Times: true, Keys: true}
	msgs := []message.Message{
		{
			Offset: 0,
			Time:   time.Date(2022, 04, 03, 14, 58, 0, 0, time.UTC),
			Key:    []byte("key"),
			Value:  []byte("value"),
		},
	}

	t.Run("RemovesOrphan", func(t *testing.T) {
		dir := t.TempDir()

		// Both .log and .segment exist for same offset.
		// Delete the index between writes to match the Migrate() invariant.
		oldSeg := New(dir, 0, message.FormatLog)
		writeMessages(t, oldSeg, params, msgs)
		require.NoError(t, os.Remove(oldSeg.Index))
		newSeg := New(dir, 0, message.FormatSegment)
		writeMessages(t, newSeg, params, msgs)

		require.NoError(t, CleanupOrphanLogs(dir))

		_, err := os.Stat(oldSeg.Data)
		require.ErrorIs(t, err, os.ErrNotExist, ".log orphan should be removed")
		_, err = os.Stat(newSeg.Data)
		require.NoError(t, err, ".segment should remain")
	})

	t.Run("NoOrphan", func(t *testing.T) {
		dir := t.TempDir()

		// Only .segment exists — no .log orphan
		seg := New(dir, 0, message.FormatSegment)
		writeMessages(t, seg, params, msgs)

		require.NoError(t, CleanupOrphanLogs(dir))

		_, err := os.Stat(seg.Data)
		require.NoError(t, err, ".segment should remain")
	})

	t.Run("Empty", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, CleanupOrphanLogs(dir))
	})
}

func TestRecoverDir(t *testing.T) {
	t.Run("Missing", func(t *testing.T) {
		dir := t.TempDir()
		missing := filepath.Join(dir, "abc")
		require.NoError(t, RecoverDir(missing, index.Params{}))
	})

	t.Run("Empty", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, RecoverDir(dir, index.Params{}))
	})
}
