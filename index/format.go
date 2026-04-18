package index

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/klev-dev/klevdb/pkg/kdir"
)

var ErrCorrupted = errors.New("index corrupted")
var errIndexSize = fmt.Errorf("%w: unaligned index size", ErrCorrupted)

var magic = [6]byte{0xFF, 'k', 'l', 'e', 'v', 'i'}

const HeaderSize = int64(len(magic) + 2) // magic + version + params

func headerNew(v version, opts Params) []byte {
	h := make([]byte, HeaderSize)
	copy(h, magic[:])
	h[len(magic)] = byte(v)

	if opts.Times {
		h[len(magic)+1] |= timesBit
	}

	if opts.Keys {
		h[len(magic)+1] |= keysBit
	}

	return h
}

func headerParse(h []byte, opts Params) (version, error) {
	data, magicFound := bytes.CutPrefix(h, magic[:])
	switch {
	case !magicFound:
		return v0, fmt.Errorf("%w: magic prefix not found", ErrCorrupted) // TODO specific error
	case data[0] > byte(vLast):
		return v0, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	case opts.Times != ((data[1] & timesBit) == timesBit):
		return v0, fmt.Errorf("%w: times index mismatch", ErrCorrupted) // TODO specific error
	case opts.Keys != ((data[1] & keysBit) == keysBit):
		return v0, fmt.Errorf("%w: keys index mismatch", ErrCorrupted) // TODO specific error
	case (data[1] & unusedBits) > 0:
		return v0, fmt.Errorf("%w: invalid reserved data", ErrCorrupted) // TODO specific error
	}
	return version(data[0]), nil
}

type version byte

const (
	v0    version = 0
	v1    version = 1
	vLast version = v1 // always last version
)

const timesBit byte = 0b00000001
const keysBit byte = 0b00000010
const unusedBits byte = 0b11111100

type DetectedFormat struct {
	name      string
	hasHeader bool
	version
}

var (
	FormatLog     = DetectedFormat{name: "log", hasHeader: false, version: v0}
	FormatSegment = DetectedFormat{name: "segment", hasHeader: true, version: vLast}
)

type Writer struct {
	opts    Params
	f       *os.File
	pos     int64
	buff    []byte
	version version
	writer  func(Item) error
}

func OpenWriter(path string, opts Params, format DetectedFormat) (w *Writer, retErr error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("write index open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("write index stat: %w", err)
	}

	if stat.Size() == 0 {
		if err := kdir.SyncParent(path); err != nil {
			return nil, fmt.Errorf("write index dir sync: %w", err)
		}
	}

	pos := stat.Size()
	version := format.version
	if format.hasHeader {
		if pos == 0 {
			h := headerNew(format.version, opts)
			if _, err := f.Write(h[:]); err != nil {
				return nil, fmt.Errorf("write index header: %w", err)
			}
			pos = int64(len(h))
		} else {
			fr, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("write index read header: %w", err)
			}
			defer func() { _ = fr.Close() }()

			var h [HeaderSize]byte
			if _, err := fr.ReadAt(h[:], 0); err != nil {
				return nil, fmt.Errorf("write index read header: %w", err)
			}
			version, err = headerParse(h[:], opts)
			if err != nil {
				return nil, fmt.Errorf("write index parse header: %w", err)
			}
		}
	}

	w = &Writer{opts: opts, f: f, pos: pos, version: version}

	switch {
	case opts.Times && opts.Keys:
		w.writer = w.writeFull
	case opts.Times:
		w.writer = w.writeTimes
	case opts.Keys:
		w.writer = w.writeKeys
	default:
		w.writer = w.writeBase
	}

	return w, nil
}

func (w *Writer) Write(it Item) error {
	return w.writer(it)
}

func (w *Writer) writeBase(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, 16)
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))

	if n, err := w.f.Write(w.buff); err != nil {
		return fmt.Errorf("write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer) writeTimes(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, 24)
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))
	binary.BigEndian.PutUint64(w.buff[16:], uint64(it.Timestamp))

	if n, err := w.f.Write(w.buff); err != nil {
		return fmt.Errorf("write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer) writeKeys(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, 24)
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))
	binary.BigEndian.PutUint64(w.buff[16:], it.KeyHash)

	if n, err := w.f.Write(w.buff); err != nil {
		return fmt.Errorf("write index: %w", err)
	} else {
		w.pos += int64(n)
	}

	return nil
}

func (w *Writer) writeFull(it Item) error {
	if w.buff == nil {
		w.buff = make([]byte, 32)
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(it.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(it.Position))
	binary.BigEndian.PutUint64(w.buff[16:], uint64(it.Timestamp))
	binary.BigEndian.PutUint64(w.buff[24:], it.KeyHash)

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

func Write(path string, opts Params, index []Item, format DetectedFormat) (retErr error) {
	w, err := OpenWriter(path, opts, format)
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

func Read(path string, opts Params, format DetectedFormat) ([]Item, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read index open: %w", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := os.Stat(path)
	if err != nil {
		return nil, fmt.Errorf("read index stat: %w", err)
	}
	dataSize := stat.Size()

	v := v0
	if format.hasHeader {
		var h [HeaderSize]byte
		if _, err := io.ReadFull(f, h[:]); err != nil {
			return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
		}
		v, err = headerParse(h[:], opts)
		if err != nil {
			return nil, fmt.Errorf("parse index header: %w", err)
		}
		dataSize -= int64(len(h))
	}
	// v0 and v1 have identical item encoding; the version field is reserved for
	// a future item format change. Add a new case here before changing item encoding.
	switch v {
	case v0, v1:
	default:
		return nil, fmt.Errorf("%w: unknown version %d", ErrCorrupted, v)
	}

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
