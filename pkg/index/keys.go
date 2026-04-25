package index

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb/pkg/message"
)

var ErrKeyNotFound = fmt.Errorf("key: %w", message.ErrNotFound)

func KeyHash(key []byte) uint64 {
	hasher := fnv.New64a()
	hasher.Write(key)
	return hasher.Sum64()
}

func KeyHashEncoded(h uint64) []byte {
	hash := make([]byte, 8)
	binary.BigEndian.PutUint64(hash, h)
	return hash
}

type keyPositions struct {
	positions []int64
}

func AppendKeys(keys art.Tree, items []Item) {
	hash := make([]byte, 8)
	for _, item := range items {
		binary.BigEndian.PutUint64(hash, item.KeyHash)

		if v, found := keys.Search(hash); found {
			kp := v.(*keyPositions)
			kp.positions = append(kp.positions, item.Position)
		} else {
			keys.Insert(hash, &keyPositions{[]int64{item.Position}})
		}
	}
}

func Keys(keys art.Tree, keyHash []byte) ([]int64, error) {
	if v, found := keys.Search(keyHash); found {
		kp := v.(*keyPositions)
		return kp.positions, nil
	}

	return nil, ErrKeyNotFound
}
