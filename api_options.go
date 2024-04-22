package klevdb

import "context"

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
	// DefaultConsume is used when consume options is nil
	DefaultConsume ConsumeOptions
}

type ConsumeOptions struct {
	MaxMessages int64
	Blocking    *ConsumeBlockingOptions
}

type ConsumeBlockingOptions struct {
	Blocking        bool
	BlockingContext context.Context
}
