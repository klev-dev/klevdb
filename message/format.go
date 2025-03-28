package message

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"golang.org/x/exp/mmap"
)

var ErrCorrupted = errors.New("log corrupted")
var errShortHeader = fmt.Errorf("%w: short header", ErrCorrupted)
var errShortMessage = fmt.Errorf("%w: short message", ErrCorrupted)
var errNoMessage = fmt.Errorf("%w: no message", ErrCorrupted)
var errCrcFailed = fmt.Errorf("%w: crc failed", ErrCorrupted)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

func Size(m Message) int64 {
	return int64(28 + len(m.Key) + len(m.Value))
}

type Writer struct {
	Path string
	f    *os.File
	pos  int64
	buff []byte
}

func OpenWriter(path string) (*Writer, error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("write log open: %w", err)
	}

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("write log stat: %w", err)
	}

	return &Writer{Path: path, f: f, pos: stat.Size()}, nil
}

func (w *Writer) Write(m Message) (int64, error) {
	var fullSize = 8 + // offset
		8 + // unix micro
		4 + // key size
		4 + // value size
		4 + // crc
		len(m.Key) + len(m.Value)

	if w.buff == nil || cap(w.buff) < fullSize {
		w.buff = make([]byte, fullSize)
	} else {
		w.buff = w.buff[:fullSize]
	}

	binary.BigEndian.PutUint64(w.buff[0:], uint64(m.Offset))
	binary.BigEndian.PutUint64(w.buff[8:], uint64(m.Time.UnixMicro()))
	binary.BigEndian.PutUint32(w.buff[16:], uint32(len(m.Key)))
	binary.BigEndian.PutUint32(w.buff[20:], uint32(len(m.Value)))

	copy(w.buff[28:], m.Key)
	copy(w.buff[28+len(m.Key):], m.Value)

	crc := crc32.Checksum(w.buff[28:], crc32cTable)
	binary.BigEndian.PutUint32(w.buff[24:], crc)

	pos := w.pos
	if n, err := w.f.Write(w.buff); err != nil {
		return 0, fmt.Errorf("write log: %w", err)
	} else {
		w.pos += int64(n)
	}
	return pos, nil
}

func (w *Writer) Size() int64 {
	return w.pos
}

func (w *Writer) Sync() error {
	if err := w.f.Sync(); err != nil {
		return fmt.Errorf("write log sync: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return fmt.Errorf("write log close: %w", err)
	}
	return nil
}

func (w *Writer) SyncAndClose() error {
	if err := w.Sync(); err != nil {
		return err
	}
	return w.Close()
}

type Reader struct {
	Path string
	r    *os.File
	ra   *mmap.ReaderAt
}

func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read log open: %w", err)
	}

	return &Reader{
		Path: path,
		r:    f,
	}, nil
}

func OpenReaderMem(path string) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read mem log open: %w", err)
	}

	return &Reader{
		Path: path,
		ra:   f,
	}, nil
}

func (r *Reader) Consume(position, maxPosition int64, maxCount int64) ([]Message, error) {
	var msgs = make([]Message, int(maxCount))
	var i int64
	for ; i < maxCount && position <= maxPosition; i++ {
		next, err := r.read(position, &msgs[i])
		switch {
		case err == nil:
			position = next
		case errors.Is(err, io.EOF):
			return msgs[:i], nil
		default:
			return nil, err
		}
	}
	return msgs[:i], nil
}

func (r *Reader) Get(position int64) (msg Message, err error) {
	_, err = r.read(position, &msg)
	return
}

func (r *Reader) Read(position int64) (msg Message, nextPosition int64, err error) {
	nextPosition, err = r.read(position, &msg)
	return
}

func (r *Reader) read(position int64, msg *Message) (nextPosition int64, err error) {
	var headerBytes [8 + 8 + 4 + 4 + 4]byte
	if r.ra != nil {
		_, err = r.ra.ReadAt(headerBytes[:], position)
	} else {
		_, err = r.r.ReadAt(headerBytes[:], position)
	}
	switch {
	case err == nil:
		// all good, continue
	case errors.Is(err, io.ErrUnexpectedEOF):
		return -1, errShortHeader
	default:
		return -1, fmt.Errorf("read header: %w", err)
	}

	msg.Offset = int64(binary.BigEndian.Uint64(headerBytes[0:]))
	msg.Time = time.UnixMicro(int64(binary.BigEndian.Uint64(headerBytes[8:]))).UTC()
	keySize := int32(binary.BigEndian.Uint32(headerBytes[16:]))
	valueSize := int32(binary.BigEndian.Uint32(headerBytes[20:]))
	expectedCRC := binary.BigEndian.Uint32(headerBytes[24:])

	position += int64(len(headerBytes))
	messageBytes := make([]byte, keySize+valueSize)
	if r.ra != nil {
		_, err = r.ra.ReadAt(messageBytes, position)
	} else {
		_, err = r.r.ReadAt(messageBytes, position)
	}
	switch {
	case err == nil:
		// all good, continue
	case errors.Is(err, io.ErrUnexpectedEOF):
		return -1, errShortMessage
	case errors.Is(err, io.EOF):
		return -1, errNoMessage
	default:
		return -1, fmt.Errorf("read message: %w", err)
	}

	actualCRC := crc32.Checksum(messageBytes, crc32cTable)
	if expectedCRC != actualCRC {
		return -1, errCrcFailed
	}

	if keySize > 0 {
		msg.Key = messageBytes[:keySize]
	}
	if valueSize > 0 {
		msg.Value = messageBytes[keySize:]
	}

	position += int64(len(messageBytes))
	return position, nil
}

func (r *Reader) Close() error {
	if r.ra != nil {
		if err := r.ra.Close(); err != nil {
			return fmt.Errorf("write mem log close: %w", err)
		}
	} else {
		if err := r.r.Close(); err != nil {
			return fmt.Errorf("write log close: %w", err)
		}
	}
	return nil
}
