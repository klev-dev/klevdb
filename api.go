package klevdb

import (
	"errors"
	"time"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
)

const OffsetOldest = message.OffsetOldest
const OffsetNewest = message.OffsetNewest
const OffsetInvalid = message.OffsetInvalid

type Message = message.Message

var InvalidMessage = message.Invalid
var ErrInvalidOffset = message.ErrInvalidOffset
var ErrNotFound = message.ErrNotFound
var ErrNoIndex = errors.New("no index")
var ErrReadonly = errors.New("log opened in readonly mode")

type Stats = segment.Stats

type Options struct {
	CreateDirs bool
	Readonly   bool
	KeyIndex   bool
	TimeIndex  bool
	AutoSync   bool
	Rollover   int64
	Check      bool
}

type Log interface {
	// Publish appends messages to the log.
	// It returns the offset of the next message to be appended.
	// The offset of the message is ignored, set to the actual offset.
	// If the time of the message is 0, it set to the current UTC time.
	Publish(messages []Message) (nextOffset int64, err error)

	// NextOffset returns the offset of the next message to be published.
	NextOffset() (nextOffset int64, err error)

	// Consume retrieves messages from the log, starting at the offset.
	// It returns offset, which can be used to retrieve for the next consume.
	// If offset == OffsetOldest, the first message will be the oldest
	//   message still available on the log. If the log is empty,
	//   it will return no error, nextOffset will be 0
	// If offset == OffsetNewest, no actual messages will be returned,
	//   but nextOffset will be set to the offset that will be used
	//   for the next Publish call
	// If offset is before the first available on the log, or is after
	//   NextOffset, it returns ErrInvalidOffset
	// If the exact offset is already deleted, it will start consuming
	//   from the next available offset.
	// Consume is allowed to return no messages, but with increasing nextOffset
	//   in case messages between offset and nextOffset have been deleted.
	// NextOffset is always bigger then offset, unless we are caught up
	//   to the head of the log in which case they are equal.
	Consume(offset int64, maxCount int64) (nextOffset int64, messages []Message, err error)

	// Get retrieves a single message, by its offset
	// If offset == OffsetOldest, it returns the first message on the log
	// If offset == OffsetNewest, it returns the last message on the log
	// If offset is before the first available on the log, or is after
	//   NextOffset, it returns ErrInvalidOffset
	// If log is empty, it returns ErrInvalidOffset
	// If the exact offset have been deleted, it returns ErrNotFound
	Get(offset int64) (message Message, err error)

	// GetByKey retrieves the last message in the log for this key
	// If no such message is found, it returns ErrNotFound
	GetByKey(key []byte) (message Message, err error)
	// OffsetByKey retrieves the last message offset in the log for this key
	// If no such message is found, it returns ErrNotFound
	OffsetByKey(key []byte) (offset int64, err error)

	// GetByTime retrieves the first message after start time
	// If start time is after all messages in the log, it returns ErrNotFound
	GetByTime(start time.Time) (message Message, err error)
	// OffsetByTime retrieves the first message offset and its time after start time
	// If start time is after all messages in the log, it returns ErrNotFound
	OffsetByTime(start time.Time) (offset int64, messageTime time.Time, err error)

	// Delete tries to delete a set of messages by their offset
	//   from the log and returns the amount of storage deleted
	// It does not guarantee that it will delete all messages,
	//   it returns the set of actually deleted offsets.
	Delete(offsets map[int64]struct{}) (deletedOffsets map[int64]struct{}, deletedSize int64, err error)

	// Size returns the amount of storage associated with a message
	Size(m Message) int64

	// Stat returns log stats like disk space, number of messages
	Stat() (Stats, error)

	// Backup takes a backup snapshot of this log to another location
	Backup(dir string) error

	// Sync forces persisting data to the disk
	Sync() error

	// Close closes the log
	Close() error
}

func Stat(dir string, opts Options) (Stats, error) {
	return segment.StatDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}

func Backup(src, dst string) error {
	return segment.BackupDir(src, dst)
}

func Check(dir string, opts Options) error {
	return segment.CheckDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}

func Recover(dir string, opts Options) error {
	return segment.RecoverDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}
