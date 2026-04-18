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
var errIndexSize = fmt.Errorf("%w: unaligned index size", ErrCorrupted)

var magic = [6]byte{0xFF, 'k', 'l', 'e', 'v', 'i'}

const HeaderSize = int64(len(magic) + 2) // magic + version + params

type Version struct {
	marker  byte
	invalid bool
}

var (
	VUnknown         = Version{invalid: true}
	V1               = Version{marker: 255}
	V2               = Version{marker: 1}
	VLast    Version = V2 // always last version
)

func (v Version) newHeader(opts Params) ([]byte, error) {
	switch v {
	case V1:
		return nil, nil
	case V2:
		h := make([]byte, HeaderSize)
		copy(h, magic[:])
		h[len(magic)] = byte(v.marker)

		if opts.Times {
			h[len(magic)+1] |= timesBit
		}

		if opts.Keys {
			h[len(magic)+1] |= keysBit
		}

		return h, nil
	default:
		return nil, fmt.Errorf("unknown version: %v", v)
	}
}

func headerParse(h []byte, offset int64, opts Params) (Version, error) {
	data, magicFound := bytes.CutPrefix(h, magic[:])
	switch {
	case !magicFound:
		if int64(binary.BigEndian.Uint64(h)) == offset {
			return V1, nil
		}
		return VUnknown, fmt.Errorf("%w: magic prefix not found", ErrCorrupted) // TODO specific error
	case data[0] > byte(VLast.marker):
		return VUnknown, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	case opts.Times != ((data[1] & timesBit) == timesBit):
		return VUnknown, fmt.Errorf("%w: times index mismatch", ErrCorrupted) // TODO specific error
	case opts.Keys != ((data[1] & keysBit) == keysBit):
		return VUnknown, fmt.Errorf("%w: keys index mismatch", ErrCorrupted) // TODO specific error
	case (data[1] & unusedBits) > 0:
		return VUnknown, fmt.Errorf("%w: invalid reserved data", ErrCorrupted) // TODO specific error
	case data[0] == V2.marker:
		return V2, nil
	default:
		return VUnknown, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	}
}

const timesBit byte = 0b00000001
const keysBit byte = 0b00000010
const unusedBits byte = 0b11111100

type Writer struct {
	opts    Params
	f       *os.File
	pos     int64
	buff    []byte
	version Version
	writer  func(Item) error
}

func OpenWriter(path string, offset int64, newVersion Version, opts Params) (w *Writer, retErr error) {
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

	pos := stat.Size()
	var v Version
	if pos == 0 {
		h, err := newVersion.newHeader(opts) // TODO configure upgrades?
		if err != nil {
			return nil, fmt.Errorf("write index header: %w", err)
		}
		if _, err := f.Write(h[:]); err != nil {
			return nil, fmt.Errorf("write index header: %w", err)
		}
		pos = int64(len(h))
		v = newVersion
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
		v, err = headerParse(h[:], offset, opts)
		if err != nil {
			return nil, fmt.Errorf("write index parse header: %w", err)
		}
	}

	w = &Writer{opts: opts, f: f, pos: pos, version: v}

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

func Write(path string, offset int64, newVersion Version, opts Params, index []Item) (retErr error) {
	w, err := OpenWriter(path, offset, newVersion, opts)
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

func Read(path string, offset int64, opts Params) ([]Item, error) {
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

	var h [HeaderSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
	}
	v, err := headerParse(h[:], offset, opts)
	if err != nil {
		return nil, fmt.Errorf("parse index header: %w", err)
	}

	switch v {
	case V1:
		if _, err := f.Seek(0, io.SeekStart); err != nil {
			return nil, fmt.Errorf("read index seek: %w", err)
		}
	case V2:
		dataSize -= int64(len(h))
	default:
		return nil, fmt.Errorf("%w: unknown version %d", ErrCorrupted, v.marker)
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
	for i := range items { // TODO potentially reader func
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

func Stat(path string, offset int64, opts Params) (int64, int, error) {
	f, err := os.Open(path)
	if err != nil {
		return -1, -1, fmt.Errorf("read index open: %w", err)
	}
	defer func() { _ = f.Close() }()

	stat, err := os.Stat(path)
	if err != nil {
		return -1, -1, fmt.Errorf("read index stat: %w", err)
	}
	dataSize := stat.Size()

	var h [HeaderSize]byte
	if _, err := io.ReadFull(f, h[:]); err != nil {
		return -1, -1, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
	}
	v, err := headerParse(h[:], offset, opts)
	if err != nil {
		return -1, -1, fmt.Errorf("parse index header: %w", err)
	}

	switch v {
	case V1:
		return dataSize, int(dataSize / opts.Size()), nil
	case V2:
		return dataSize, int((dataSize - int64(len(h))) / opts.Size()), nil
	default:
		return -1, -1, fmt.Errorf("%w: unknown version %d", ErrCorrupted, v.marker)
	}
}
