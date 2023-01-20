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