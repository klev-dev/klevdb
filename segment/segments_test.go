package segment

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/index"
)

func TestRecoverDir(t *testing.T) {
	t.Run("Missing", func(t *testing.T) {
		dir := t.TempDir()
		missing := filepath.Join(dir, "abc")
		require.NoError(t, RecoverDir(missing, index.OffsetIndex{}))
	})

	t.Run("Empty", func(t *testing.T) {
		dir := t.TempDir()
		require.NoError(t, RecoverDir(dir, index.OffsetIndex{}))
	})
}
