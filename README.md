# klevdb

[![CI](https://github.com/klev-dev/klevdb/actions/workflows/ci.yml/badge.svg)](https://github.com/klev-dev/klevdb/actions)
[![Go Reference](https://pkg.go.dev/badge/github.com/klev-dev/klevdb.svg)](https://pkg.go.dev/github.com/klev-dev/klevdb)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

klevdb is a fast message store, written in Go. Think single partition on Kafka, but stored locally.

In addition to basic consuming by offset, you can also configure klevdb to index times and keys. Time indexes allow you to quickly find a message by its time (or the first message after a certain time). Key indexes allow you to quickly find the last message with a given key.

## Usage

To add klevdb to your package use:

```
go get github.com/klev-dev/klevdb
```

To use klevdb:

```
package main

import (
    "fmt"

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
...
BenchmarkSingle/Publish/V2/1/No-8               575226	      2473 ns/op	  76.82 MB/s	     119 B/op	       0 allocs/op
BenchmarkSingle/Publish/V2/1/Times-8         	  669390	      2649 ns/op	  74.75 MB/s	     119 B/op	       0 allocs/op
BenchmarkSingle/Publish/V2/1/Keys-8          	  538255	      2918 ns/op	  67.85 MB/s	     262 B/op	       6 allocs/op
BenchmarkSingle/Publish/V2/1/All-8           	  515534	      2898 ns/op	  71.08 MB/s	     262 B/op	       6 allocs/op
BenchmarkSingle/Publish/V2/8/No-8            	  641661	      2531 ns/op	 600.64 MB/s	     150 B/op	       0 allocs/op
BenchmarkSingle/Publish/V2/8/Times-8         	  674102	      2566 ns/op	 617.35 MB/s	     150 B/op	       0 allocs/op
BenchmarkSingle/Publish/V2/8/Keys-8          	  583078	      2780 ns/op	 569.76 MB/s	     287 B/op	       5 allocs/op
BenchmarkSingle/Publish/V2/8/All-8           	  576492	      2738 ns/op	 601.84 MB/s	     287 B/op	       5 allocs/op
...
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~400,000 messages/sec, no indexes
 * ~350,000 messages/sec, with all indexes enabled
 * scales linearly with the batch size

### Consume
```
≻ make bench-consume 
...
BenchmarkSingle/Consume/V2/W/No/1-8         	 3940876	       291.1 ns/op	 652.67 MB/s	     256 B/op	       2 allocs/op
BenchmarkSingle/Consume/V2/W/No/8-8         	10298083	       124.1 ns/op	12248.89 MB/s	     264 B/op	       1 allocs/op
...
BenchmarkSingle/Consume/V2/W/All/1-8         	 4214562	       300.9 ns/op	 684.58 MB/s	     256 B/op	       2 allocs/op
BenchmarkSingle/Consume/V2/W/All/8-8         	 7985094	       163.2 ns/op	10100.82 MB/s	     264 B/op	       1 allocs/op
...
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~3,500,000 messages/sec, single message consume
 * ~8,000,000 messages/sec, in 8 message batch consume

### Get
```
≻ make bench-get
...
BenchmarkSingle/Get/V2/ByOffset-8         	 3820048	       295.7 ns/op	 642.63 MB/s	     256 B/op	       2 allocs/op
BenchmarkSingle/Get/V2/ByKey/W-8          	 1000000	      3363 ns/op	  58.88 MB/s	     264 B/op	       3 allocs/op
BenchmarkSingle/Get/V2/ByKey/A-8          	 1000000	      3634 ns/op	  54.49 MB/s	     456 B/op	       8 allocs/op
BenchmarkSingle/Get/V2/ByTime/W-8         	 1000000	      2499 ns/op	  79.24 MB/s	     256 B/op	       2 allocs/op
BenchmarkSingle/Get/V2/ByTime/A-8         	 1000000	      2374 ns/op	  83.42 MB/s	     313 B/op	       2 allocs/op
...
```
With default rollover of 1MB, for messages with keys 10B and values 128B:
 * ~3,300,000 gets/sec, across all offsets
 * ~290,000 key reads/sec, across all keys
 * ~420,000 time reads/sec, across all times

### Multi
```
≻ make bench-multi
BenchmarkMulti/Base/V2-8      	  442836	      3155 ns/op	                   570 B/op	       6 allocs/op
BenchmarkMulti/Publish/V2-8   	   45343	     26558 ns/op	  31.03 MB/s	    2807 B/op	      56 allocs/op
BenchmarkMulti/Consume/V2-8   	 1707186	     681.4 ns/op	1162.23 MB/s	    2559 B/op	      12 allocs/op
BenchmarkMulti/GetKey/V2-8    	  288034	      4286 ns/op	  48.06 MB/s	    2640 B/op	      30 allocs/op
...
```

## Future ideas

 - cmd to interact with logs
 - unified key index - an index across all segments
 - unified time index - across all segments
 - lightweight logs - delay opening the writer
 - multi index store
 - remove interfaces
