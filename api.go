package klevdb

import (
	"errors"
	"time"

	"github.com/klev-dev/klevdb/index"
	"github.com/klev-dev/klevdb/message"
	"github.com/klev-dev/klevdb/segment"
)

const (
	// OffsetOldest represents the smallest offset still available
	// Use it to consume all messages, starting at the first available
	OffsetOldest = message.OffsetOldest
	// OffsetNewest represents the offset that will be used for the next produce
	// Use it to consume only new messages
	OffsetNewest = message.OffsetNewest
	// OffsetInvalid is the offset returned when error is detected
	OffsetInvalid = message.OffsetInvalid
)

type Message = message.Message

// InvalidMessage returned when an error have occurred
var InvalidMessage = message.Invalid

// ErrInvalidOffset error is returned when the offset attribute is invalid or out of bounds
var ErrInvalidOffset = message.ErrInvalidOffset

// ErrNotFound error is returned when the offset, key or timestamp is not found
var ErrNotFound = message.ErrNotFound

// ErrNoIndex error is returned when we try to use key or timestamp, but the log doesn't include index on them
var ErrNoIndex = errors.New("no index")

// ErrReadonly error is returned when attempting to modify (e.g. publish or delete) from a log that is open as a readonly
var ErrReadonly = errors.New("log opened in readonly mode")

type Stats = segment.Stats

type Options struct {
	// When set will try to create all directories
	CreateDirs bool
	// Open the store in readonly mode
	Readonly bool
	// Index message keys, enabling GetByKey and OffsetByKey
	KeyIndex bool
	// Index message times, enabling GetByTime and OffsetByTime
	TimeIndex bool
	// Force filesystem sync after each Publish
	AutoSync bool
	// At what segment size it will rollover to a new segment. Defaults to 1mb.
	Rollover int64
	// Check the head segment for integrity, before opening it for reading/writing.
	Check bool
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
	Consume(offset int64, options ...ConsumeOption) (nextOffset int64, messages []Message, err error)

	// ConsumeByKey is similar to Consume, but only returns messages matching the key
	ConsumeByKey(key []byte, offset int64, options ...ConsumeOption) (nextOffset int64, messages []Message, err error)

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

	// Sync forces persisting data to the disk. It returns the nextOffset
	// at the time of the Sync, so clients can determine what portion
	// of the log is now durable.
	Sync() (nextOffset int64, err error)

	// GC releases any unused resources associated with this log
	GC(unusedFor time.Duration) error

	// Close closes the log
	Close() error
}

// Stat stats a store directory, without opening the store
func Stat(dir string, opts Options) (Stats, error) {
	return segment.StatDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}

// Backup backups a store directory to another location, without opening the store
func Backup(src, dst string) error {
	return segment.BackupDir(src, dst)
}

// Check runs an integrity check, without opening the store
func Check(dir string, opts Options) error {
	return segment.CheckDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}

// Recover rewrites the storage to include all messages prior the first that fails an integrity check
func Recover(dir string, opts Options) error {
	return segment.RecoverDir(dir, index.Params{
		Times: opts.TimeIndex,
		Keys:  opts.KeyIndex,
	})
}
