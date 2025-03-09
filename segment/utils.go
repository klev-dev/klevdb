package segment

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"

	"github.com/mr-tron/base58"
)

func randStr(length int) (string, error) {
	k := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return "", fmt.Errorf("[segment.rand] read full: %w", err)
	}
	return base58.Encode(k), nil
}

func copyFile(src, dst string) error {
	fsrc, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("[segment.copy] %s open src: %w", src, err)
	}
	defer fsrc.Close()

	stat, err := fsrc.Stat()
	if err != nil {
		return fmt.Errorf("[segment.copy] %s stat src: %w", src, err)
	}

	fdst, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		switch dstStat, err := os.Stat(dst); {
		case err != nil:
			return fmt.Errorf("[segment.copy] %s stat dst: %w", dst, err)
		case stat.Size() == dstStat.Size() && stat.ModTime().Equal(dstStat.ModTime()):
			// TODO do we need a safer version of this?
			return nil
		}
		fdst, err = os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	}
	if err != nil {
		return fmt.Errorf("[segment.copy] %s open dst: %w", dst, err)
	}
	defer fdst.Close()

	switch n, err := io.Copy(fdst, fsrc); {
	case err != nil:
		return fmt.Errorf("[segment.copy] %s copy to %s: %w", src, dst, err)
	case n < stat.Size():
		return fmt.Errorf("[segment.copy] %s (%d) incomplete copy to %s (%d): %w", src, stat.Size(), dst, n, err)
	}

	if err := fdst.Sync(); err != nil {
		return fmt.Errorf("[segment.copy] %s sync: %w", dst, err)
	}
	if err := fdst.Close(); err != nil {
		return fmt.Errorf("[segment.copy] %s close: %w", dst, err)
	}
	if err := os.Chtimes(dst, stat.ModTime(), stat.ModTime()); err != nil {
		return fmt.Errorf("[segment.copy] %s chtimes: %w", dst, err)
	}

	return nil
}
