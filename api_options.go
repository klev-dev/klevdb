package klevdb

import "context"

type ConsumeOptions struct {
	MaxMessages     int64
	Blocking        bool
	BlockingContext context.Context
}

type ConsumeOption interface {
	Apply(*ConsumeOptions)
}

type ConsumeOptionFunc func(*ConsumeOptions)

func (f ConsumeOptionFunc) Apply(opts *ConsumeOptions) {
	f(opts)
}

func ConsumeMaxMessages(maxMessages int64) ConsumeOptionFunc {
	return func(opts *ConsumeOptions) {
		opts.MaxMessages = maxMessages
	}
}

func ConsumeBlocking(ctx context.Context) ConsumeOptionFunc {
	return func(opts *ConsumeOptions) {
		opts.Blocking = true
		opts.BlockingContext = ctx
	}
}
