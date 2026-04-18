package kdir

import (
	"os"
	"path/filepath"
)

func SyncParent(path string) error {
	return Sync(filepath.Dir(path))
}

func Sync(dir string) (retErr error) {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() {
		if err := f.Close(); retErr == nil {
			retErr = err
		}
	}()
	return f.Sync()
}
