package shard

import (
	"../arraypool"
	"encoding/binary"
	"hash/fnv"
	"sync"
)

type Shard struct {
	arrayPool          *arraypool.ArrayPool
	mapping            map[uint32][]byte
	mutex              sync.Mutex
	hits               int64
	updatedEntriesChan chan<- EntryStatus
}

func createNewShard(initialSize int64, baseSegmentSize int, updatedEntriesChan chan<- EntryStatus) (*Shard, error) {
	arrPool := arraypool.NewArrayPool(baseSegmentSize)

	return &Shard{
		arrayPool:          arrPool,
		mapping:            make(map[uint32][]byte, initialSize),
		updatedEntriesChan: updatedEntriesChan,
	}, nil
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (s *Shard) Add(key string, value []byte) {

	hashedKey := hashKey(key)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if a entry already exists
	if bytes, ok := s.mapping[hashedKey]; ok {
		s.arrayPool.Release(bytes)
	}

	data := s.arrayPool.Rent(len(value) + 8)

	binary.LittleEndian.PutUint64(data, uint64(len(value)))

	copy(data[8:], value)

	s.hits += 1

	s.mapping[hashedKey] = data

	s.onEntryUpdated(hashedKey, false)
}

func (s *Shard) Get(key string) []byte {
	hashedKey := hashKey(key)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if bytes, ok := s.mapping[hashedKey]; ok {

		lengthArr := bytes[:8]

		length := binary.LittleEndian.Uint64(lengthArr)

		return bytes[8 : 8+length]
	}

	return nil
}

func (s *Shard) onEntryUpdated(key uint32, deleted bool) {
	s.updatedEntriesChan <- EntryStatus{hashedKey: key, deleted: deleted}
}
