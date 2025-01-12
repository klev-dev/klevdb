package index

import (
	"encoding/binary"
	"hash/fnv"

	art "github.com/plar/go-adaptive-radix-tree/v2"

	"github.com/klev-dev/klevdb/message"
)

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

func AppendKeys(keys art.Tree, items []Item) {
	hash := make([]byte, 8)
	for _, item := range items {
		binary.BigEndian.PutUint64(hash, item.KeyHash)

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
