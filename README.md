# klevdb

![CI](https://github.com/klev-dev/klevdb/actions/workflows/ci.yml/badge.svg)

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
goos: linux
goarch: amd64
pkg: github.com/klev-dev/klevdb
cpu: 11th Gen Intel(R) Core(TM) i5-1135G7 @ 2.40GHz
```

### Publish
```
≻ make bench-publish 
go test -bench=BenchmarkSingle/Publish -benchmem

BenchmarkSingle/Publish/1/No-8    	  347466	      3337 ns/op	  54.54 MB/s	     156 B/op	       1 allocs/op
BenchmarkSingle/Publish/1/Times-8 	  391308	      3585 ns/op	  53.00 MB/s	     162 B/op	       1 allocs/op
BenchmarkSingle/Publish/1/Keys-8  	  314779	      3960 ns/op	  47.98 MB/s	     305 B/op	       7 allocs/op
BenchmarkSingle/Publish/1/All-8   	  319302	      3907 ns/op	  50.68 MB/s	     310 B/op	       7 allocs/op
BenchmarkSingle/Publish/8/No-8    	  397518	      3266 ns/op	 445.81 MB/s	     156 B/op	       0 allocs/op
BenchmarkSingle/Publish/8/Times-8 	  451623	      3402 ns/op	 446.73 MB/s	     161 B/op	       0 allocs/op
BenchmarkSingle/Publish/8/Keys-8  	  309150	      3821 ns/op	 397.78 MB/s	     298 B/op	       5 allocs/op
BenchmarkSingle/Publish/8/All-8   	  382129	      3797 ns/op	 417.17 MB/s	     303 B/op	       5 allocs/op

PASS
ok  	github.com/klev-dev/klevdb	12.433s
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~300,000 writes/sec, no indexes
 * ~250,000 writes/sec, with all indexes enabled
 * scales lineary with the batch size

### Consume
```
≻ make bench-consume 

BenchmarkSingle/Consume/W/1-8     	 4372142	       279.5 ns/op	 651.05 MB/s	     224 B/op	       2 allocs/op
BenchmarkSingle/Consume/RW/1-8    	 4377028	       287.1 ns/op	 633.94 MB/s	     274 B/op	       2 allocs/op
BenchmarkSingle/Consume/R/1-8     	 4356441	       299.0 ns/op	 608.71 MB/s	     274 B/op	       2 allocs/op
BenchmarkSingle/Consume/W/8-8     	 6508213	       178.4 ns/op	8163.31 MB/s	     294 B/op	       1 allocs/op
BenchmarkSingle/Consume/RW/8-8    	 6069168	       194.8 ns/op	7475.85 MB/s	     344 B/op	       1 allocs/op
BenchmarkSingle/Consume/R/8-8     	 6271984	       196.4 ns/op	7413.22 MB/s	     344 B/op	       1 allocs/op

PASS
ok  	github.com/klev-dev/klevdb	147.152s
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~3,500,000 reads/sec, single message consume
 * ~5,500,000 reads/sec, 8 message batches

### Get
```
≻ make bench-get
go test -bench=BenchmarkSingle/Get -benchmem

BenchmarkSingle/Get/ByOffset-8         	 5355378	       225.2 ns/op	 808.24 MB/s	     144 B/op	       1 allocs/op
BenchmarkSingle/Get/ByKey-8            	 1000000	      3583 ns/op	  53.04 MB/s	     152 B/op	       2 allocs/op
BenchmarkSingle/Get/ByKey/R-8          	 1000000	      3794 ns/op	  50.08 MB/s	     345 B/op	       7 allocs/op
BenchmarkSingle/Get/ByTime-8           	 1000000	      2197 ns/op	  86.48 MB/s	     144 B/op	       1 allocs/op
BenchmarkSingle/Get/ByTime/R-8         	 1000000	      2178 ns/op	  87.25 MB/s	     202 B/op	       1 allocs/op

PASS
ok  	github.com/klev-dev/klevdb	52.528s
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~4,400,000 gets/sec, across all offsets
 * ~270,000 key reads/sec, across all keys
 * ~450,000 time reads/sec, across all times

### Multi
```
≻ make bench-multi
go test -bench=BenchmarkMulti -benchmem

BenchmarkMulti/Base-8         	  282462	      4433 ns/op	     673 B/op	       7 allocs/op
BenchmarkMulti/Publish-8      	   30628	     40717 ns/op	  19.45 MB/s	    2974 B/op	      56 allocs/op
BenchmarkMulti/Consume-8      	 1289114	       909.9 ns/op	 835.24 MB/s	    2842 B/op	      17 allocs/op
BenchmarkMulti/GetKey-8       	  459753	      5729 ns/op	  34.56 MB/s	    1520 B/op	      20 allocs/op

PASS
ok  	github.com/klev-dev/klevdb	22.973s
```
