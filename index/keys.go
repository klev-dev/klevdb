package index

import (
	"hash/fnv"

	art "github.com/plar/go-adaptive-radix-tree"

	"github.com/klev-dev/klevdb/message"
)

func KeyHash(key []byte) []byte {
	hash := make([]byte, 8)
	hasher := fnv.New64a()
	hasher.Write(key)
	hasher.Sum(hash[:0])
	return hash
}

func AppendKeys(keys art.Tree, items []Item) {
	for _, item := range items {
		hash := item.KeyHash[:]

		var positions []int64
		if v, found := keys.Search(hash); found {
			positions = v.([]int64)
		}
		positions = append(positions, item.Position)

		keys.Insert(hash, positions)
	}
}

func Keys(keys art.Tree, keyHash []byte) ([]int64, error) {
	if v, found := keys.Search(keyHash); found {
		return v.([]int64), nil
	}

	return nil, message.ErrNotFound
}
