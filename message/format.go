package message

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
	"time"

	"golang.org/x/exp/mmap"

	"github.com/klev-dev/kleverr"
)

var ErrCorrupted = errors.New("message corrupted")

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
		return nil, kleverr.Newf("could not open log: %w", err)
	}

	pos := int64(0)
	if stat, err := f.Stat(); err != nil {
		return nil, kleverr.Newf("could not stat log: %w", err)
	} else {
		pos = stat.Size()
	}

	return &Writer{Path: path, f: f, pos: pos}, nil
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
		return 0, kleverr.Newf("log failed write: %w", err)
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
		return kleverr.Newf("could not sync log: %w", err)
	}
	return nil
}

func (w *Writer) Close() error {
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close log: %w", err)
	}
	return nil
}

func (w *Writer) SyncAndClose() error {
	if err := w.f.Sync(); err != nil {
		return kleverr.Newf("could not sync log: %w", err)
	}
	if err := w.f.Close(); err != nil {
		return kleverr.Newf("could not close log: %w", err)
	}
	return nil
}

type Reader struct {
	Path string
	r    *os.File
	ra   *mmap.ReaderAt
}

func OpenReader(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, kleverr.Newf("could not open log: %w", err)
	}

	return &Reader{
		Path: path,
		r:    f,
	}, nil
}

func OpenReaderMem(path string) (*Reader, error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, kleverr.Newf("could not open log: %w", err)
	}

	return &Reader{
		Path: path,
		ra:   f,
	}, nil
}

func (r *Reader) Consume(position int64, maxCount int64) ([]Message, error) {
	var msgs []Message
	for int64(len(msgs)) < maxCount {
		msg, next, err := r.Read(position)
		if errors.Is(err, io.EOF) {
			break
		} else if err != nil {
			return nil, err
		}
		msgs = append(msgs, msg)
		position = next
	}
	return msgs, nil
}

func (r *Reader) Get(position int64) (Message, error) {
	msg, _, err := r.Read(position)
	return msg, err
}

var retReadErr = kleverr.Ret2[Message, int64]

func (r *Reader) Read(position int64) (Message, int64, error) {
	var err error

	var headerBytes = make([]byte, 8+8+4+4+4)
	if r.ra != nil {
		_, err = r.ra.ReadAt(headerBytes, position)
	} else {
		_, err = r.r.ReadAt(headerBytes, position)
	}
	switch {
	case errors.Is(err, io.ErrUnexpectedEOF):
		return retReadErr(kleverr.Newf("%w: short header", ErrCorrupted))
	case err != nil:
		return retReadErr(kleverr.Newf("could not read header: %w", err))
	}

	offset := int64(binary.BigEndian.Uint64(headerBytes[0:]))
	ts := time.UnixMicro(int64(binary.BigEndian.Uint64(headerBytes[8:]))).UTC()
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
	case errors.Is(err, io.ErrUnexpectedEOF):
		return retReadErr(kleverr.Newf("%w: short message", ErrCorrupted))
	case errors.Is(err, io.EOF):
		return retReadErr(kleverr.Newf("%w: no message", ErrCorrupted))
	case err != nil:
		return retReadErr(kleverr.Newf("could not read message: %w", err))
	}

	actualCRC := crc32.Checksum(messageBytes, crc32cTable)
	if expectedCRC != actualCRC {
		return retReadErr(kleverr.Newf("%w: invalid crc: expected=%d; actual=%d", ErrCorrupted, expectedCRC, actualCRC))
	}

	var msg = Message{Offset: offset, Time: ts}
	if keySize > 0 {
		msg.Key = messageBytes[:keySize]
	}
	if valueSize > 0 {
		msg.Value = messageBytes[keySize:]
	}

	position += int64(len(messageBytes))
	return msg, position, nil
}

func (r *Reader) Close() error {
	if r.ra != nil {
		if err := r.ra.Close(); err != nil {
			return kleverr.Newf("could not close log: %w", err)
		}
	} else {
		if err := r.r.Close(); err != nil {
			return kleverr.Newf("could not close log: %w", err)
		}
	}
	return nil
}
