package shard

import (
	"encoding/binary"
	"hash/fnv"
	"sync"
	"time"

	"github.com/mohsenShakiba/distributed-cache/pkg/arraypool"
	"github.com/mohsenShakiba/distributed-cache/pkg/storage"
)

type Entry struct {
	key     string
	value   []byte
	exp     time.Time
	deleted bool
}

type Shard struct {
	arrayPool          *arraypool.ArrayPool
	mapping            map[uint32]Entry
	mutex              sync.Mutex
	hits               int64
	updatedEntriesChan chan<- *storage.CacheEntry
}

func NewShard(initialSize int64, baseSegmentSize int, updatedEntriesChan chan<- *storage.CacheEntry) (*Shard, error) {
	arrPool := arraypool.NewArrayPool(baseSegmentSize)

	return &Shard{
		arrayPool:          arrPool,
		mapping:            make(map[uint32]Entry, initialSize),
		updatedEntriesChan: updatedEntriesChan,
	}, nil
}

func hashKey(key string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(key))
	return h.Sum32()
}

func (s *Shard) Add(key string, value []byte, expiration time.Time) {

	hashedKey := hashKey(key)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	// check if a entry already exists
	if val, ok := s.mapping[hashedKey]; ok {
		s.arrayPool.Release(val.value)
	}

	data := s.arrayPool.Rent(len(value) + 8)

	binary.LittleEndian.PutUint64(data, uint64(len(value)))

	copy(data[8:], value)

	s.hits += 1

	entry := Entry{key: key, value: data, exp: expiration, deleted: false}

	s.mapping[hashedKey] = entry

	s.onEntryUpdated(entry)
}

func (s *Shard) Get(key string) []byte {
	hashedKey := hashKey(key)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	if val, ok := s.mapping[hashedKey]; ok {

		lengthArr := val.value[:8]

		if val.exp.Sub(time.Now()) < 0 {
			return nil
		}

		length := binary.LittleEndian.Uint64(lengthArr)

		return val.value[8 : 8+length]
	}

	return nil
}

func (s *Shard) Delete(key string) {

	hashedKey := hashKey(key)

	s.mutex.Lock()
	defer s.mutex.Unlock()

	delete(s.mapping, hashedKey)

	entry := Entry{key: key, value: nil, exp: time.Now(), deleted: true}

	s.onEntryUpdated(entry)

}

func (s *Shard) onEntryUpdated(e Entry) {
	s.updatedEntriesChan <- &storage.CacheEntry{
		Key:        e.key,
		Deleted:    e.deleted,
		Expiration: e.exp,
		Value:      e.value,
	}
}

func (s *Shard) getAllCacheEntries() []*storage.CacheEntry {
	s.mutex.Lock()
	defer s.mutex.Unlock()

	entries := make([]*storage.CacheEntry, 0, 1000)

	for _, val := range s.mapping {
		entries = append(entries, &storage.CacheEntry{
			Key:        val.key,
			Deleted:    val.deleted,
			Expiration: val.exp,
			Value:      val.value,
		})
	}

	return entries
}
