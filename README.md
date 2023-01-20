# klevdb

klevdb is a fast message store, written in Go. Think single partition on kafka, but stored locally.

In addition to basic consuming by offset, you can also configure klevdb to index times and keys. Times index allow you to quickly find a message by its time (or the first message after a certain time). Keys index allow you to quickly find the last message with a given key.

## Usage

To add klevdb to your package use:

```
go get github.com/klev-dev/klevdb
```

To use klevdb:

```
package main

import (
    "github.com/klev-dev/klevdb"
)

func main() {
	db, _ := klevdb.Open("/tmp/kdb", klevdb.Options{
		CreateDirs: true,
		KeyIndex:   true,
	})
	defer db.Close()

	publishNext, _ := db.Publish([]klevdb.Message{
		{
			Key:   []byte("key1"),
			Value: []byte("val1"),
		},
		{
			Key:   []byte("key1"),
			Value: []byte("val2"),
		},
	})
	fmt.Println("published, next offset:", publishNext)

	consumeNext, msgs, _ := db.Consume(klevdb.OffsetOldest, 1)
	fmt.Println("consumed:", msgs, "value:", string(msgs[0].Value))
	fmt.Println("next consume offset:", consumeNext)

	msg, _ := db.GetByKey([]byte("key1"))
	fmt.Println("got:", msg, "value:", string(msg.Value))
}
```

Running the above program, outputs the following:
```
published, next offset: 2
consumed: [{0 2009-11-10 23:00:00 +0000 UTC [107 101 121 49] [118 97 108 49]}] value: val1
next consume offset: 1
got: {1 2009-11-10 23:00:00 +0000 UTC [107 101 121 49] [118 97 108 50]} value: val2
```

Further documentation is available at [GoDoc](https://pkg.go.dev/github.com/klev-dev/klevdb)

## Performance

Benchmarks on framework gen1 i5:
```
≻ make bench
go test -bench=. -benchmem
goos: linux
goarch: amd64
pkg: github.com/klev-dev/klevdb
cpu: 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz
BenchmarkSinlge/PublishBase-8         	  352294	      3340 ns/op	  54.49 MB/s	     156 B/op	       1 allocs/op
BenchmarkSinlge/PublishIndex-8        	  374749	      3981 ns/op	  49.73 MB/s	     310 B/op	       7 allocs/op
BenchmarkSinlge/PublishBatch-8        	  367899	      3814 ns/op	 207.63 MB/s	     304 B/op	       5 allocs/op
BenchmarkSinlge/ConsumeBase-8         	 5781859	       213.0 ns/op	3418.32 MB/s	     284 B/op	       1 allocs/op
BenchmarkSinlge/ConsumeReopen-8       	 5403850	       217.4 ns/op	3348.96 MB/s	     334 B/op	       1 allocs/op
BenchmarkSinlge/ConsumeRO-8           	 5371581	       220.6 ns/op	3300.65 MB/s	     334 B/op	       1 allocs/op
BenchmarkSinlge/GetBase-8             	 5413568	       227.6 ns/op	 799.50 MB/s	     144 B/op	       1 allocs/op
BenchmarkSinlge/GetKeyBase-8          	 1000000	      3906 ns/op	  48.65 MB/s	     152 B/op	       2 allocs/op
BenchmarkSinlge/ConcurrentBase-8      	  296805	      4472 ns/op	                    1498 B/op	      14 allocs/op
BenchmarkMulti/Publish-8              	   30901	     39878 ns/op	  19.86 MB/s	    2977 B/op	      56 allocs/op
BenchmarkMulti/Consume-8              	 1125958	       934.1 ns/op	 813.57 MB/s	    2842 B/op	      17 allocs/op
BenchmarkMulti/GetKey-8               	  448561	      5917 ns/op	  33.46 MB/s	    1520 B/op	      20 allocs/op
PASS
ok  	github.com/klev-dev/klevdb	129.748s
```

With default options, for messages with keys 10B and values 128B:
 * ~300,000 writes/sec
   * ~250,000 writes/sec, with all indexes enabled
 * ~4,500,000 reads/sec
   * ~250,000 key reads/sec, across all possible keys