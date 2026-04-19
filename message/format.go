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

const HeaderSize = int64(len(magic) + 2) // magic + Version byte + reserved byte

type Version struct {
	marker byte
}

var (
	VUnknown         = Version{}
	V1               = Version{marker: 255}
	V2               = Version{marker: 1}
	VLast    Version = V2 // always last version
)

func (v Version) String() string {
	switch v {
	case V1:
		return "V1"
	case V2:
		return "V2"
	default:
		return fmt.Sprintf("Version(unknown:%d)", v.marker)
	}
}

func (v Version) newHeader() ([]byte, error) {
	switch v {
	case V1:
		return nil, nil
	case V2:
		h := make([]byte, HeaderSize)
		copy(h, magic[:])
		h[len(magic)] = byte(v.marker)
		h[len(magic)+1] = 0
		return h, nil
	default:
		return nil, fmt.Errorf("unknown version: %v", v)
	}
}

func headerParse(h []byte, offset int64) (Version, error) {
	data, magicFound := bytes.CutPrefix(h, magic[:])
	switch {
	case !magicFound:
		if int64(binary.BigEndian.Uint64(h)) == offset {
			return V1, nil
		}
		return VUnknown, fmt.Errorf("%w: magic prefix not found", ErrCorrupted) // TODO specific error
	case data[0] > byte(VLast.marker):
		return VUnknown, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	case data[1] != 0:
		return VUnknown, fmt.Errorf("%w: invalid reserved data", ErrCorrupted) // TODO specific error
	case data[0] == V2.marker:
		return V2, nil
	default:
		return VUnknown, fmt.Errorf("%w: unknown version %d", ErrCorrupted, data[0]) // TODO specific error
	}
}

func Size(m Message, v Version) int64 {
	switch v {
	case V1:
		return int64(28 + len(m.Key) + len(m.Value))
	case V2:
		return int64(fixedSize + len(m.Key) + len(m.Value))
	default:
		panic(fmt.Sprintf("unknown version: %v", v))
	}
}

type Writer struct {
	Path    string
	f       *os.File
	pos     int64
	buff    []byte
	version Version
	writer  func(m Message) (int64, error)
}

func OpenWriter(path string, offset int64, newVersion Version) (w *Writer, retErr error) {
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

	pos := stat.Size()
	var v Version
	if pos == 0 {
		h, err := newVersion.newHeader()
		if err != nil {
			return nil, fmt.Errorf("write log header: %w", err)
		}
		if _, err := f.Write(h[:]); err != nil {
			return nil, fmt.Errorf("write log header: %w", err)
		}
		pos = int64(len(h))
		v = newVersion
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
		v, err = headerParse(h[:], offset)
		if err != nil {
			return nil, fmt.Errorf("write log parse header: %w", err)
		}
	}

	w = &Writer{Path: path, f: f, pos: pos, version: v}
	switch v {
	case V1:
		w.writer = w.writeV1
	case V2:
		w.writer = w.writeV2
	default:
		return nil, fmt.Errorf("unknown version: %v", v)
	}
	return w, nil
}

func (w *Writer) Write(m Message) (int64, error) {
	return w.writer(m)
}

func (w *Writer) writeV1(m Message) (int64, error) {
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

var trailerMagicData = binary.BigEndian.AppendUint64(nil, trailerMagic)

const (
	msgHeaderSize = 4 + 8 + 8 + 4 + 4 // 28: crc + offset + unixmicro + keylen + valuelen
	trailerSize   = 8
	fixedSize     = msgHeaderSize + trailerSize // 36 total overhead

	headerPayloadSize  = msgHeaderSize - 4 // 24: Offset+UnixMicro+KeyLen+ValueLen
	maxMessageBodySize = 64 * 1024 * 1024  // 64 MiB: guard against corrupt size fields causing huge allocation
)

func (w *Writer) writeV2(m Message) (int64, error) {
	fullSize := fixedSize + len(m.Key) + len(m.Value)

	if w.buff == nil || cap(w.buff) < fullSize {
		w.buff = make([]byte, fullSize)
	} else {
		w.buff = w.buff[:fullSize]
	}

	// buf[0:4] left for CRC (written last)
	binary.BigEndian.PutUint64(w.buff[4:], uint64(m.Offset))
	binary.BigEndian.PutUint64(w.buff[12:], uint64(m.Time.UnixMicro()))
	binary.BigEndian.PutUint32(w.buff[20:], uint32(len(m.Key)))
	binary.BigEndian.PutUint32(w.buff[24:], uint32(len(m.Value)))
	copy(w.buff[msgHeaderSize:], m.Key)
	copy(w.buff[msgHeaderSize+len(m.Key):], m.Value)
	copy(w.buff[msgHeaderSize+len(m.Key)+len(m.Value):], trailerMagicData)

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
	v      Version // TODO make public
	reader func(position int64, msg *Message) (nextPosition int64, err error)
}

func OpenReader(path string, offset int64) (r *Reader, retErr error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read log open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	var h [HeaderSize]byte
	var v Version
	if n, err := f.ReadAt(h[:], 0); err != nil {
		if !errors.Is(err, io.EOF) || n != 0 {
			return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
		}
		v = V1 // empty file: V1 has no file header
	} else {
		var err error
		v, err = headerParse(h[:], offset)
		if err != nil {
			return nil, fmt.Errorf("parse log header: %w", err)
		}
	}

	r = &Reader{Path: path, r: f, v: v}
	switch v {
	case V1:
		r.reader = r.readV1
	case V2:
		r.reader = r.readV2
	default:
		return nil, fmt.Errorf("read log invalid version: %v", v)
	}
	return r, nil
}

func OpenReaderMem(path string, offset int64) (r *Reader, retErr error) {
	f, err := mmap.Open(path)
	if err != nil {
		return nil, fmt.Errorf("read mem log open: %w", err)
	}
	defer func() {
		if retErr != nil {
			_ = f.Close()
		}
	}()

	var h [HeaderSize]byte
	var v Version
	if n, err := f.ReadAt(h[:], 0); err != nil {
		if !errors.Is(err, io.EOF) || n != 0 {
			return nil, fmt.Errorf("%w: reading header: %w", ErrCorrupted, err)
		}
		v = V1 // empty file: V1 has no file header
	} else {
		var err error
		v, err = headerParse(h[:], offset)
		if err != nil {
			return nil, fmt.Errorf("parse log header: %w", err)
		}
	}

	r = &Reader{Path: path, ra: f, v: v}
	switch v {
	case V1:
		r.reader = r.readV1
	case V2:
		r.reader = r.readV2
	default:
		return nil, fmt.Errorf("read log invalid version: %v", v)
	}
	return r, nil
}

func (r *Reader) Version() Version {
	return r.v
}

func (r *Reader) InitialPosition() int64 {
	switch r.v {
	case V1:
		return 0
	case V2:
		return int64(HeaderSize)
	default:
		panic(fmt.Sprintf("unknown version: %v", r.v))
	}
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

func (r *Reader) readV1(position int64, msg *Message) (nextPosition int64, err error) {
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

func (r *Reader) readV2(position int64, msg *Message) (nextPosition int64, err error) {
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
	msg.Offset = int64(binary.BigEndian.Uint64(headerBytes[4:]))
	msg.Time = time.UnixMicro(int64(binary.BigEndian.Uint64(headerBytes[12:]))).UTC()
	keySize := int32(binary.BigEndian.Uint32(headerBytes[20:]))
	valueSize := int32(binary.BigEndian.Uint32(headerBytes[24:]))

	// Step 3: Validate sizes
	if keySize < 0 || valueSize < 0 {
		return -1, errInvalidHeader
	}
	if int(keySize)+int(valueSize) > maxMessageBodySize { // TODO maybe externally configurable per log + default
		return -1, errInvalidHeader
	}

	// Step 4: Allocate payload = headerBytes[4:] (24 bytes) ++ key ++ value ++ trailer.
	// Combining them avoids passing a stack-allocated slice to crc32, which would
	// cause headerBytes to escape to the heap and add an extra allocation per read.
	payloadSize := headerPayloadSize + int(keySize) + int(valueSize) + trailerSize
	payload := make([]byte, payloadSize)
	copy(payload[:headerPayloadSize], headerBytes[4:])
	if r.ra != nil {
		_, err = r.ra.ReadAt(payload[headerPayloadSize:], position+msgHeaderSize)
	} else {
		_, err = r.r.ReadAt(payload[headerPayloadSize:], position+msgHeaderSize)
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

	// Step 5: Verify CRC over the combined payload (already heap-allocated, no escape)
	actualCRC := crc32.Checksum(payload, crc32cTable)
	if expectedCRC != actualCRC {
		return -1, errCrcFailed
	}

	// Step 6: Verify trailer
	trailerOff := headerPayloadSize + int(keySize) + int(valueSize)
	if !bytes.Equal(payload[trailerOff:], trailerMagicData) {
		return -1, errBadTrailer
	}

	// Step 7: Assign key/value
	if keySize > 0 {
		msg.Key = payload[headerPayloadSize : headerPayloadSize+int(keySize)]
	}
	if valueSize > 0 {
		msg.Value = payload[headerPayloadSize+int(keySize) : headerPayloadSize+int(keySize)+int(valueSize)]
	}

	return position + int64(msgHeaderSize) + int64(int(keySize)+int(valueSize)+trailerSize), nil
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
