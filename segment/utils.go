package segment

import (
	"crypto/rand"
	"io"
	"os"

	"github.com/klev-dev/kleverr"
	"github.com/mr-tron/base58"
)

func randStr(length int) (string, error) {
	k := make([]byte, length)
	if _, err := io.ReadFull(rand.Reader, k); err != nil {
		return "", kleverr.Ret(err)
	}
	return base58.Encode(k), nil
}

func copyFile(src, dst string) error {
	fsrc, err := os.Open(src)
	if err != nil {
		return kleverr.Newf("could not open src: %w", err)
	}
	defer fsrc.Close()

	stat, err := fsrc.Stat()
	if err != nil {
		return kleverr.Newf("could not stat src: %w", err)
	}

	fdst, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if os.IsExist(err) {
		switch dstStat, err := os.Stat(dst); {
		case err != nil:
			return kleverr.Newf("could not stat dst: %w", err)
		case stat.Size() == dstStat.Size() && stat.ModTime().Equal(dstStat.ModTime()):
			// TODO do we need a safer version of this?
			return nil
		}
		fdst, err = os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0600)
	}
	if err != nil {
		return kleverr.Newf("could not open dst: %w", err)
	}
	defer fdst.Close()

	switch n, err := io.Copy(fdst, fsrc); {
	case err != nil:
		return kleverr.Newf("could not copy: %w", err)
	case n < stat.Size():
		return kleverr.Newf("could not copy all data (%d/%d)", n, stat.Size())
	}

	if err := fdst.Sync(); err != nil {
		return kleverr.Newf("could not sync dst: %w", err)
	}
	if err := fdst.Close(); err != nil {
		return kleverr.Newf("could not close dst: %w", err)
	}
	if err := os.Chtimes(dst, stat.ModTime(), stat.ModTime()); err != nil {
		return kleverr.Newf("could not set dst time: %w", err)
	}

	return nil
}
