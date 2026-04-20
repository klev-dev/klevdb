package klevdb

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/klev-dev/klevdb/pkg/message"
)

var v1opts = VersionOptions{NewSegmentsVersion: V1}
var v2opts = VersionOptions{}

func BenchmarkSingle(b *testing.B) {
	b.Run("Publish", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkPublish(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkPublish(b, v1opts) })
	})
	b.Run("Consume", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkConsume(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkConsume(b, v1opts) })
	})
	b.Run("Get", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkGet(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkGet(b, v1opts) })
	})
}

func BenchmarkMulti(b *testing.B) {
	b.Run("Base", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkBaseMulti(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkBaseMulti(b, v1opts) })
	})
	b.Run("Publish", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkPublishMulti(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkPublishMulti(b, v1opts) })
	})
	b.Run("Consume", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkConsumeMulti(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkConsumeMulti(b, v1opts) })
	})
	b.Run("GetKey", func(b *testing.B) {
		b.Run("V2", func(b *testing.B) { benchmarkGetKeyMulti(b, v2opts) })
		b.Run("V1", func(b *testing.B) { benchmarkGetKeyMulti(b, v1opts) })
	})
}

func MkdirBench(b *testing.B) string {
	name := strings.ReplaceAll(b.Name(), "/", "_")

	currentDir, err := os.Getwd()
	require.NoError(b, err)

	dir, err := os.MkdirTemp(currentDir, name)
	require.NoError(b, err)
	return dir
}

func benchmarkPublish(b *testing.B, version VersionOptions) {
	var cases = []struct {
		name string
		opts Options
	}{
		{"No", Options{Version: version}},
		{"Times", Options{TimeIndex: true, Version: version}},
		{"Keys", Options{KeyIndex: true, Version: version}},
		{"All", Options{TimeIndex: true, KeyIndex: true, Version: version}},
	}
	for _, bn := range []int{1, 8} {
		for _, c := range cases {
			b.Run(fmt.Sprintf("%d/%s", bn, c.name), func(b *testing.B) {
				dir := MkdirBench(b)
				defer os.RemoveAll(dir)

				s, err := Open(dir, c.opts)
				require.NoError(b, err)
				defer s.Close()

				msgs := message.Gen(b.N)

				b.SetBytes(s.Size(msgs[0]) * int64(bn))
				b.ResetTimer()

				for i := 0; i < b.N; i += bn {
					top := min(i+bn, b.N)

					if _, err := s.Publish(msgs[i:top]); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
			})
		}
	}
}

func benchmarkPublishMulti(b *testing.B, version VersionOptions) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{TimeIndex: true, KeyIndex: true, Version: version})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	g := new(errgroup.Group)
	for range 10 {
		g.Go(func() error {
			for i := 0; i < b.N; i += 4 {
				top := min(i+4, b.N)
				if _, err := s.Publish(msgs[i:top]); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(b, g.Wait())

	b.StopTimer()
}

func fillLog(b *testing.B, l Log) []Message {
	msgs := message.Gen(b.N)
	for i := 0; i < b.N; i += 4 {
		top := min(i+4, b.N)
		if _, err := l.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}
	return msgs
}

func benchmarkConsume(b *testing.B, version VersionOptions) {
	var cases = []struct {
		name string
		opts Options
	}{
		{"No", Options{Version: version}},
		{"Times", Options{TimeIndex: true, Version: version}},
		{"Keys", Options{KeyIndex: true, Version: version}},
		{"All", Options{TimeIndex: true, KeyIndex: true, Version: version}},
	}
	for _, bn := range []int{1, 8} {
		for _, c := range cases {
			b.Run(fmt.Sprintf("W/%s/%d", c.name, bn), func(b *testing.B) {
				dir := MkdirBench(b)
				defer os.RemoveAll(dir)

				l, err := Open(dir, c.opts)
				require.NoError(b, err)
				defer l.Close()

				msgs := fillLog(b, l)

				b.SetBytes(l.Size(msgs[0]) * int64(bn))
				b.ResetTimer()

				for i := 0; i < b.N; i += bn {
					if _, _, err := l.Consume(int64(i), int64(bn)); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
			})

			b.Run(fmt.Sprintf("A/%s/%d", c.name, bn), func(b *testing.B) {
				dir := MkdirBench(b)
				defer os.RemoveAll(dir)

				l, err := Open(dir, c.opts)
				require.NoError(b, err)
				defer l.Close()

				msgs := fillLog(b, l)
				require.NoError(b, l.Close())

				b.SetBytes(l.Size(msgs[0]) * int64(bn))
				b.ResetTimer()

				l, err = Open(dir, c.opts)
				require.NoError(b, err)
				defer l.Close()

				for i := 0; i < b.N; i += bn {
					if _, _, err := l.Consume(int64(i), int64(bn)); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
			})

			b.Run(fmt.Sprintf("R/%s/%d", c.name, bn), func(b *testing.B) {
				dir := MkdirBench(b)
				defer os.RemoveAll(dir)

				l, err := Open(dir, c.opts)
				require.NoError(b, err)
				defer l.Close()

				msgs := fillLog(b, l)
				require.NoError(b, l.Close())

				b.SetBytes(l.Size(msgs[0]) * int64(bn))
				b.ResetTimer()

				opts := c.opts
				opts.Readonly = true
				l, err = Open(dir, opts)
				require.NoError(b, err)
				defer l.Close()

				for i := 0; i < b.N; i += bn {
					if _, _, err := l.Consume(int64(i), int64(bn)); err != nil {
						b.Fatal(err)
					}
				}

				b.StopTimer()
			})
		}
	}
}

func benchmarkConsumeMulti(b *testing.B, version VersionOptions) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true, Version: version})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := range msgs {
		if _, err := s.Publish(msgs[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	g := new(errgroup.Group)
	for range 10 {
		g.Go(func() error {
			for i := 0; i < b.N; i += 4 {
				if _, _, err := s.Consume(int64(i), 4); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(b, g.Wait())

	b.StopTimer()
}

func benchmarkGet(b *testing.B, version VersionOptions) {
	b.Run("ByOffset", func(b *testing.B) {
		dir := MkdirBench(b)
		defer os.RemoveAll(dir)

		l, err := Open(dir, Options{Version: version})
		require.NoError(b, err)
		defer l.Close()

		msgs := fillLog(b, l)

		b.SetBytes(l.Size(msgs[0]))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := l.Get(int64(i)); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	b.Run("ByKey/W", func(b *testing.B) {
		dir := MkdirBench(b)
		defer os.RemoveAll(dir)

		l, err := Open(dir, Options{KeyIndex: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		msgs := fillLog(b, l)

		b.SetBytes(l.Size(msgs[0]))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := l.GetByKey(msgs[i].Key); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	b.Run("ByKey/A", func(b *testing.B) {
		dir := MkdirBench(b)
		defer os.RemoveAll(dir)

		l, err := Open(dir, Options{KeyIndex: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		msgs := fillLog(b, l)
		require.NoError(b, l.Close())

		b.SetBytes(l.Size(msgs[0]))
		b.ResetTimer()

		l, err = Open(dir, Options{KeyIndex: true, Readonly: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		for i := 0; i < b.N; i++ {
			if _, err := l.GetByKey(msgs[i].Key); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	b.Run("ByTime/W", func(b *testing.B) {
		dir := MkdirBench(b)
		defer os.RemoveAll(dir)

		l, err := Open(dir, Options{TimeIndex: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		msgs := fillLog(b, l)

		b.SetBytes(l.Size(msgs[0]))
		b.ResetTimer()

		for i := 0; i < b.N; i++ {
			if _, err := l.GetByTime(msgs[i].Time); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})

	b.Run("ByTime/A", func(b *testing.B) {
		dir := MkdirBench(b)
		defer os.RemoveAll(dir)

		l, err := Open(dir, Options{TimeIndex: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		msgs := fillLog(b, l)
		require.NoError(b, l.Close())

		b.SetBytes(l.Size(msgs[0]))
		b.ResetTimer()

		l, err = Open(dir, Options{TimeIndex: true, Readonly: true, Version: version})
		require.NoError(b, err)
		defer l.Close()

		for i := 0; i < b.N; i++ {
			if _, err := l.GetByTime(msgs[i].Time); err != nil {
				b.Fatal(err)
			}
		}

		b.StopTimer()
	})
}

func benchmarkGetKeyMulti(b *testing.B, version VersionOptions) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true, Version: version})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := 0; i < b.N; i += 10 {
		top := min(i+10, b.N)
		if _, err := s.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	g := new(errgroup.Group)
	for range 10 {
		g.Go(func() error {
			for i := 0; i < b.N; i++ {
				if _, err := s.GetByKey(msgs[i].Key); err != nil {
					return err
				}
			}
			return nil
		})
	}
	require.NoError(b, g.Wait())

	b.StopTimer()
}

func benchmarkBaseMulti(b *testing.B, version VersionOptions) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true, Version: version})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.ResetTimer()

	g := new(errgroup.Group)

	g.Go(func() error {
		for i := 0; i < b.N; i += 10 {
			top := min(i+10, b.N)
			if _, err := s.Publish(msgs[i:top]); err != nil {
				return err
			}
		}
		return nil
	})

	g.Go(func() error {
		offset := OffsetOldest
		for offset < int64(len(msgs)) {
			next, _, err := s.Consume(offset, 10)
			if err != nil {
				return err
			}
			offset = next
		}
		return nil
	})
	require.NoError(b, g.Wait())

	b.StopTimer()
}
