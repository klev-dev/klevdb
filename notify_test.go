package klevdb

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestNotify(t *testing.T) {
	t.Run("unblock", func(t *testing.T) {
		n := NewOffsetNotify(10)

		err := n.Wait(context.TODO(), 5)
		require.NoError(t, err)
	})

	t.Run("blocked", func(t *testing.T) {
		n := NewOffsetNotify(10)
		ch := make(chan struct{})
		var wg sync.WaitGroup

		wg.Add(1)
		go func() {
			defer wg.Done()
			close(ch)

			err := n.Wait(context.TODO(), 15)
			require.NoError(t, err)
		}()

		<-ch
		time.Sleep(10 * time.Millisecond)
		n.Set(20)
		wg.Wait()
	})

	t.Run("cancel", func(t *testing.T) {
		n := NewOffsetNotify(10)
		ch := make(chan struct{})
		var wg sync.WaitGroup
		ctx, cancel := context.WithCancel(context.TODO())
		defer cancel()

		wg.Add(1)
		go func() {
			defer wg.Done()
			close(ch)

			err := n.Wait(ctx, 15)
			require.ErrorIs(t, err, context.Canceled)
		}()

		<-ch
		time.Sleep(10 * time.Millisecond)
		cancel()
		wg.Wait()
	})
}
