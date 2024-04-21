package klevdb

import "context"

type ConsumeOptions struct {
	MaxMessages     int64
	Blocking        bool
	BlockingContext context.Context
}
