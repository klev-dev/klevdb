package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
)

var ErrCorrupted = errors.New("index corrupted")
var ErrParamsMismatch = fmt.Errorf("%w: index params mismatch", ErrCorrupted)
var errIndexSize = fmt.Errorf("%w: unaligned index size", ErrCorrupted)

var preamble = [8]byte{
	0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF,
}

const version byte = 0b00000001
const timesBit byte = 0b00000001
const keysBit byte = 0b00000010

// HeaderSize is the number of bytes written at the start of every index file.
const HeaderSize = 8 + 2 // preamble + version + spec

type Writer struct {
	opts      Params
	f         *os.File
	pos       int64
	buff      []byte
	keyOffset int
}

func encodeHeader(opts Params) []byte {
	header := make([]byte, HeaderSize)
	copy(header, preamble[:])
	header[len(preamble)] = version
	var p byte
	if opts.Times {
		p |= timesBit
	}
	if opts.Keys {
		p |= keysBit
	}
	header[len(preamble)+1] = p
	return header
}

func OpenWriter(path string, opts Params) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("write index open: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write index stat: %w", err)
	}
	pos := stat.Size()

	if pos == 0 {
		if _, err := f.Write(encodeHeader(opts)); err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("write index header: %w", err)
		}
		pos = HeaderSize
	} else {
		rf, err := os.Open(path)
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("read index header open: %w", err)
		}
		header := make([]byte, HeaderSize)
		_, err = io.ReadFull(rf, header)
		_ = rf.Close()
		if err != nil {
			_ = f.Close()
			return nil, fmt.Errorf("read index header: %w", err)
		}
		if err := checkHeader(header, opts); err != nil {
			_ = f.Close()
			return nil, err
		}
	}

	return &Writer{
		opts:      opts,
		f:         f,
		pos:       pos,
		keyOffset: opts.keyOffset(),
	}, nil
}

func openWriterReset(path string, opts Params) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("write index open: %w", err)
	}

	if _, err := f.Write(encodeHeader(opts)); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("write index header: %w", err)
	}

	return &Writer{
		opts:      opts,
		f:         f,
		pos:       HeaderSize,
		keyOffset: opts.keyOffset(),
	}, nil
}

func (w *Writer) Write(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, w.opts.Size())
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))

	if w.opts.Times {
		binary.BigEndian.PutUint64(w.buff[16:], uint64(it.Timestamp))
	}

	if w.opts.Keys {
		binary.BigEndian.PutUint64(w.buff[w.keyOffset:], it.KeyHash)
	}

	if n, err := w.f.Write(w.buff); err != nil {
		return fmt.Errorf("write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer) Size() int64 {
	return w.pos
}

func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("write index sync: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("write index close: %w", err)
	}
	return nil
}

func (w *Writer) SyncAndClose() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.Close()
}

func Write(path string, opts Params, index []Item) (retErr error) {
	w, err := openWriterReset(path, opts)
	if err != nil {
		return err
	}
	defer func() {
		if retErr != nil {
			_ = w.Close()
		}
	}()

	for _, item := range index {
		if err := w.Write(item); err != nil {
			return err
		}
	}

	return w.SyncAndClose()
}

func Read(path string, opts Params) ([]Item, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read index open: %w", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("read index stat: %w", err)
	}
	dataSize := stat.Size()
	if dataSize == 0 {
		return nil, nil
	}

	header := make([]byte, HeaderSize)
	if _, err := io.ReadFull(f, header); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return nil, fmt.Errorf("%w: partial index header", ErrCorrupted)
		}
		return nil, fmt.Errorf("read index header: %w", err)
	} else if err := checkHeader(header, opts); err != nil {
		return nil, err
	}
	dataSize -= HeaderSize

	itemSize := opts.Size()
	if dataSize%itemSize > 0 {
		return nil, errIndexSize
	}

	data := make([]byte, dataSize)
	if _, err = io.ReadFull(f, data); err != nil {
		return nil, fmt.Errorf("read index: %w", err)
	}

	var keyOffset = opts.keyOffset()

	var items = make([]Item, dataSize/int64(itemSize))
	for i := range items {
		pos := i * int(itemSize)

		items[i].Offset = int64(binary.BigEndian.Uint64(data[pos:]))
		items[i].Position = int64(binary.BigEndian.Uint64(data[pos+8:]))

		if opts.Times {
			items[i].Timestamp = int64(binary.BigEndian.Uint64(data[pos+16:]))
		}

		if opts.Keys {
			items[i].KeyHash = binary.BigEndian.Uint64(data[pos+keyOffset:])
		}
	}
	return items, nil
}

func NeedsReindex(path string, opts Params) (bool, error) {
	switch info, err := os.Stat(path); {
	case os.IsNotExist(err):
		return true, nil
	case err != nil:
		return false, fmt.Errorf("needs reindex stat: %w", err)
	case info.Size() == 0:
		return true, nil
	default:
		f, err := os.Open(path)
		if err != nil {
			return false, fmt.Errorf("needs reindex header open: %w", err)
		}
		defer f.Close()

		header := make([]byte, HeaderSize)
		switch _, err := io.ReadFull(f, header); {
		case err == nil:
			if err := checkHeader(header, opts); err != nil {
				return true, nil
			}
			return false, nil
		case errors.Is(err, io.ErrUnexpectedEOF):
			return true, nil // index does not have enough data, needs reindex
		default:
			return false, fmt.Errorf("needs reindex header read: %w", err)
		}
	}
}

func checkHeader(header []byte, opts Params) error {
	headerData, preambleFound := bytes.CutPrefix(header, preamble[:])
	switch {
	case !preambleFound:
		return fmt.Errorf("%w: missing index header", ErrCorrupted)
	case headerData[0] != version:
		return fmt.Errorf("%w: invalid index version (%d, expected %d)", ErrCorrupted, headerData[0], version)
	case opts.Times != ((headerData[1] & timesBit) == timesBit):
		return fmt.Errorf("%w: times index mismatch", ErrParamsMismatch)
	case opts.Keys != ((headerData[1] & keysBit) == keysBit):
		return fmt.Errorf("%w: keys index mismatch", ErrParamsMismatch)
	}
	return nil
}
