package klevdb

import (
	"os"
	"strings"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/klev-dev/klevdb/message"
)

func BenchmarkSinlge(b *testing.B) {
	b.Run("PublishBase", benchmarkPublishBase)
	b.Run("PublishIndex", benchmarkPublishIndex)
	b.Run("PublishBatch", benchmarkPublishBatch)
	b.Run("ConsumeBase", benchmarkConsumeBase)
	b.Run("ConsumeReopen", benchmarkConsumeReopen)
	b.Run("ConsumeRO", benchmarkConsumeRO)
	b.Run("GetBase", benchmarkGetBase)
	b.Run("GetKeyBase", benchmarkGetKeyBase)
	b.Run("ConcurrentBase", benchmarkConcurrentBase)
}

func BenchmarkMulti(b *testing.B) {
	b.Run("Publish", benchmarkPublishMulti)
	b.Run("Consume", benchmarkConsumeMulti)
	b.Run("GetKey", benchmarkGetKeyMulti)
}

func MkdirBench(b *testing.B) string {
	parent, name, found := strings.Cut(b.Name(), "/")
	if !found {
		name = parent
	}

	currentDir, err := os.Getwd()
	require.NoError(b, err)

	dir, err := os.MkdirTemp(currentDir, name)
	require.NoError(b, err)
	return dir
}

func benchmarkPublishBase(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.Publish(msgs[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkPublishIndex(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.Publish(msgs[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkPublishBatch(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	for i := 0; i < b.N; i += 4 {
		top := i + 4
		if top > b.N {
			top = b.N
		}
		if _, err := s.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkPublishMulti(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{TimeIndex: true, KeyIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	var wg sync.WaitGroup
	for k := 0; k < 10; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i += 4 {
				top := i + 4
				if top > b.N {
					top = b.N
				}
				if _, err := s.Publish(msgs[i:top]); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	b.StopTimer()
}

func benchmarkConsumeBase(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{})
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

	for i := 0; i < b.N; i += 4 {
		if _, _, err := s.Consume(int64(i), 4); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkConsumeReopen(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := range msgs {
		if _, err := s.Publish(msgs[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}
	require.NoError(b, s.Close())

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	s, err = Open(dir, Options{})
	require.NoError(b, err)
	defer s.Close()

	for i := 0; i < b.N; i += 4 {
		if _, _, err := s.Consume(int64(i), 4); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkConsumeRO(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := range msgs {
		if _, err := s.Publish(msgs[i : i+1]); err != nil {
			b.Fatal(err)
		}
	}
	require.NoError(b, s.Close())

	b.SetBytes(s.Size(msgs[0]) * 4)
	b.ResetTimer()

	s, err = Open(dir, Options{Readonly: true})
	require.NoError(b, err)
	defer s.Close()

	for i := 0; i < b.N; i += 4 {
		if _, _, err := s.Consume(int64(i), 4); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkConsumeMulti(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true})
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

	var wg sync.WaitGroup
	for k := 0; k < 10; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i += 4 {
				if _, _, err := s.Consume(int64(i), 4); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	b.StopTimer()
}

func benchmarkGetBase(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := 0; i < b.N; i += 10 {
		top := i + 10
		if top > b.N {
			top = b.N
		}
		if _, err := s.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.Get(int64(i)); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkGetKeyBase(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := 0; i < b.N; i += 10 {
		top := i + 10
		if top > b.N {
			top = b.N
		}
		if _, err := s.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := s.GetByKey(msgs[i].Key); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
}

func benchmarkGetKeyMulti(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)
	for i := 0; i < b.N; i += 10 {
		top := i + 10
		if top > b.N {
			top = b.N
		}
		if _, err := s.Publish(msgs[i:top]); err != nil {
			b.Fatal(err)
		}
	}

	b.SetBytes(s.Size(msgs[0]))
	b.ResetTimer()

	var wg sync.WaitGroup
	for k := 0; k < 10; k++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < b.N; i++ {
				if _, err := s.GetByKey(msgs[i].Key); err != nil {
					b.Fatal(err)
				}
			}
		}()
	}
	wg.Wait()

	b.StopTimer()
}

func benchmarkConcurrentBase(b *testing.B) {
	dir := MkdirBench(b)
	defer os.RemoveAll(dir)

	s, err := Open(dir, Options{KeyIndex: true, TimeIndex: true})
	require.NoError(b, err)
	defer s.Close()

	msgs := message.Gen(b.N)

	b.ResetTimer()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for i := 0; i < b.N; i += 10 {
			top := i + 10
			if top > b.N {
				top = b.N
			}
			if _, err := s.Publish(msgs[i:top]); err != nil {
				b.Fatal(err)
			}
		}
	}()

	go func() {
		defer wg.Done()

		offset := OffsetOldest
		for offset < int64(len(msgs)) {
			next, _, err := s.Consume(offset, 10)
			require.NoError(b, err)
			offset = next
		}
	}()

	wg.Wait()

	b.StopTimer()
}
