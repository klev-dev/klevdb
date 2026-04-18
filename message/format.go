package message

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"time"

	"github.com/klev-dev/klevdb/pkg/kdir"
	"golang.org/x/exp/mmap"
)

var ErrCorrupted = errors.New("log corrupted")
var errShortHeader = fmt.Errorf("%w: short header", ErrCorrupted)
var errShortMessage = fmt.Errorf("%w: short message", ErrCorrupted)
var errShortData = fmt.Errorf("%w: short data", ErrCorrupted)
var errNoMessage = fmt.Errorf("%w: no message", ErrCorrupted)
var errInvalidHeader = fmt.Errorf("%w: invalid header", ErrCorrupted)
var errCrcFailed = fmt.Errorf("%w: crc failed", ErrCorrupted)
var errBadTrailer = fmt.Errorf("%w: bad trailer", ErrCorrupted)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

var magic = [6]byte{0xFF, 'k', 'l', 'e', 'v', 's'}

const HeaderSize = int64(len(magic) + 2) // magic + version byte + reserved byte

func headerNew(v version) []byte {
	h := make([]byte, HeaderSize)
	copy(h, magic[:])
	h[len(magic)] = byte(v)
	h[len(magic)+1] = 0
	return h
}

func headerParse(h []byte) (version, error) {
	data, magicFound := bytes.CutPrefix(h, magic[:])
	switch {
	case !magicFound:
		return v0, fmt.Errorf("%w: magic prefix not found", ErrCorrupted) // TODO specific error
	case data[0] > byte(vLast):
		return v0, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	case data[1] != 0:
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

type DetectedFormat struct {
	name      string
	hasHeader bool
	version
}

var (
	FormatLog     = DetectedFormat{name: "log", hasHeader: false, version: v0}
	FormatSegment = DetectedFormat{name: "segment", hasHeader: true, version: vLast}
)

func Size(m Message, format DetectedFormat) int64 {
	switch format {
	case FormatLog:
		return int64(28 + len(m.Key) + len(m.Value))
	default:
		return int64(fixedSize + len(m.Key) + len(m.Value))
	}
}

type Writer struct {
	Path    string
	f       *os.File
	pos     int64
	buff    []byte
	version version
	writer  func(m Message) (int64, error)
}

func OpenWriter(path string, format DetectedFormat) (w *Writer, retErr error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0600)
	if err != nil {
		return nil, fmt.Errorf("write log open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	stat, err := f.Stat()
	if err != nil {
		return nil, fmt.Errorf("write log stat: %w", err)
	}

	if stat.Size() == 0 {
		if err := kdir.SyncParent(path); err != nil {
			return nil, fmt.Errorf("write log dir sync: %w", err)
		}
	}

	pos := stat.Size()
	version := format.version
	if format.hasHeader {
		if pos == 0 {
			h := headerNew(format.version)
			if _, err := f.Write(h[:]); err != nil {
				return nil, fmt.Errorf("write log header: %w", err)
			}
			pos = int64(len(h))
		} else {
			fr, err := os.Open(path)
			if err != nil {
				return nil, fmt.Errorf("write log read header: %w", err)
			}
			defer func() { _ = fr.Close() }()

			var h [HeaderSize]byte
			if _, err := fr.ReadAt(h[:], 0); err != nil {
				return nil, fmt.Errorf("write log read header: %w", err)
			}
			version, err = headerParse(h[:])
			if err != nil {
				return nil, fmt.Errorf("write log parse header: %w", err)
			}
		}
	}

	w = &Writer{Path: path, f: f, pos: pos, version: version}
	switch version {
	case v0:
		w.writer = w.writeV0
	default:
		w.writer = w.writeV1
	}
	return w, nil
}

func (w *Writer) Write(m Message) (int64, error) {
	return w.writer(m)
}

func (w *Writer) writeV0(m Message) (int64, error) {
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

const trailerMagic uint64 = 0xDEADBEEFFEEDFACE

const (
	msgHeaderSize    = 4 + 4 + 8 + 8 + 4         // 28: crc+totallen+offset+unixmicro+valuelen
	trailerSize      = 8
	fixedSize        = msgHeaderSize + trailerSize // 36 total overhead
	totalLengthFixed = 8 + 8 + 4 + trailerSize    // 28: offset+unixmicro+valuelen+trailer (no key/val)

	maxMessageBodySize = 64 * 1024 * 1024 // 64 MiB: guard against corrupt totalLength causing huge allocation
)

func (w *Writer) writeV1(m Message) (int64, error) {
	fullSize := fixedSize + len(m.Key) + len(m.Value)

	if w.buff == nil || cap(w.buff) < fullSize {
		w.buff = make([]byte, fullSize)
	} else {
		w.buff = w.buff[:fullSize]
	}

	totalLength := uint32(totalLengthFixed + len(m.Key) + len(m.Value))
	// buf[0:4] left for CRC (written last)
	binary.BigEndian.PutUint32(w.buff[4:], totalLength)
	binary.BigEndian.PutUint64(w.buff[8:], uint64(m.Offset))
	binary.BigEndian.PutUint64(w.buff[16:], uint64(m.Time.UnixMicro()))
	binary.BigEndian.PutUint32(w.buff[24:], uint32(len(m.Value)))
	copy(w.buff[28:], m.Key)
	copy(w.buff[28+len(m.Key):], m.Value)
	trailerOff := 28 + len(m.Key) + len(m.Value)
	binary.BigEndian.PutUint64(w.buff[trailerOff:], trailerMagic)

	// CRC covers buf[4:] (everything after CRC field)
	crc := crc32.Checksum(w.buff[4:], crc32cTable)
	binary.BigEndian.PutUint32(w.buff[0:], crc)

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
	Path   string
	r      *os.File
	ra     *mmap.ReaderAt
	v      version
	reader func(position int64, msg *Message) (nextPosition int64, err error)
}

func OpenReader(path string, format DetectedFormat) (r *Reader, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read log open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	version := format.version
	if format.hasHeader {
		var h [HeaderSize]byte
		if _, err := f.ReadAt(h[:], 0); err != nil {
			return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
		}
		version, err = headerParse(h[:])
		if err != nil {
			return nil, fmt.Errorf("parse log header: %w", err)
		}
	}

	r = &Reader{Path: path, r: f, v: version}
	switch version {
	case v0:
		r.reader = r.readv0
	default:
		r.reader = r.readv1
	}
	return r, nil
}

func OpenReaderMem(path string, format DetectedFormat) (r *Reader, retErr error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read mem log open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	version := format.version
	if format.hasHeader {
		var h [HeaderSize]byte
		if _, err := f.ReadAt(h[:], 0); err != nil {
			return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
		}
		version, err = headerParse(h[:])
		if err != nil {
			return nil, fmt.Errorf("parse log header: %w", err)
		}
	}

	r = &Reader{Path: path, ra: f, v: version}
	switch version {
	case v0:
		r.reader = r.readv0
	default:
		r.reader = r.readv1
	}
	return r, nil
}

func (r *Reader) InitialPosition() int64 {
	if r.v == v0 {
		return 0
	}
	return int64(HeaderSize)
}

func (r *Reader) Consume(position, maxPosition int64, maxCount int64) ([]Message, error) {
	var msgs = make([]Message, int(maxCount))
	var i int64
	for ; i < maxCount && position <= maxPosition; i++ {
		next, err := r.reader(position, &msgs[i])
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
	_, err = r.reader(position, &msg)
	return
}

func (r *Reader) Read(position int64) (msg Message, nextPosition int64, err error) {
	nextPosition, err = r.reader(position, &msg)
	return
}

func (r *Reader) readv0(position int64, msg *Message) (nextPosition int64, err error) {
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

func (r *Reader) readv1(position int64, msg *Message) (nextPosition int64, err error) {
	// Step 1: Read 28-byte header
	var headerBytes [msgHeaderSize]byte
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

	// Step 2: Parse header
	expectedCRC := binary.BigEndian.Uint32(headerBytes[0:])
	totalLength := int32(binary.BigEndian.Uint32(headerBytes[4:]))
	msg.Offset = int64(binary.BigEndian.Uint64(headerBytes[8:]))
	msg.Time = time.UnixMicro(int64(binary.BigEndian.Uint64(headerBytes[16:]))).UTC()
	valueSize := int32(binary.BigEndian.Uint32(headerBytes[24:]))

	// Step 3: Derive key_len; validate
	keySize := totalLength - int32(totalLengthFixed) - valueSize
	if keySize < 0 || valueSize < 0 || totalLength < int32(totalLengthFixed) {
		return -1, errInvalidHeader
	}
	if int(keySize)+int(valueSize) > maxMessageBodySize {
		return -1, errInvalidHeader
	}

	// Step 4: Read key + value + trailer
	dataSize := int(keySize) + int(valueSize) + trailerSize
	dataBytes := make([]byte, dataSize)
	if r.ra != nil {
		_, err = r.ra.ReadAt(dataBytes, position+msgHeaderSize)
	} else {
		_, err = r.r.ReadAt(dataBytes, position+msgHeaderSize)
	}
	switch {
	case err == nil:
		// all good, continue
	case errors.Is(err, io.ErrUnexpectedEOF):
		return -1, errShortData
	case errors.Is(err, io.EOF):
		return -1, errShortData
	default:
		return -1, fmt.Errorf("read data: %w", err)
	}

	// Step 5: Verify CRC (chains header[4:] and dataBytes)
	partialCRC := crc32.Checksum(headerBytes[4:], crc32cTable)
	actualCRC := crc32.Update(partialCRC, crc32cTable, dataBytes)
	if expectedCRC != actualCRC {
		return -1, errCrcFailed
	}

	// Step 6: Verify trailer
	trailerOff := int(keySize) + int(valueSize)
	if binary.BigEndian.Uint64(dataBytes[trailerOff:]) != trailerMagic {
		return -1, errBadTrailer
	}

	// Step 7: Assign key/value
	if keySize > 0 {
		msg.Key = dataBytes[:keySize]
	}
	if valueSize > 0 {
		msg.Value = dataBytes[keySize : keySize+valueSize]
	}

	return position + int64(msgHeaderSize) + int64(dataSize), nil
}

func (r *Reader) Close() error {
	if r.ra != nil {
		if err := r.ra.Close(); err != nil {
			return fmt.Errorf("read mem log close: %w", err)
		}
	} else {
		if err := r.r.Close(); err != nil {
			return fmt.Errorf("read log close: %w", err)
		}
	}
	return nil
}
