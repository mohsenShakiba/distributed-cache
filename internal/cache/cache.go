package cache

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/mohsenShakiba/distributed-cache/internal/file_storage"
	"github.com/mohsenShakiba/distributed-cache/internal/shard"
	"github.com/mohsenShakiba/distributed-cache/internal/storage"
)

type Cache struct {
	segments           []*shard.Shard
	options            CacheConfig
	updatedEntriesChan chan *storage.CacheEntry
	Storage            *file_storage.FileCacheStorage
}

type CacheConfig struct {
	InitialSegmentSize int64
	NumberOfSegments   int
	SegmentWidth       int
	SyncDuration       time.Duration
	FilePath           string
	MaxFileSize        int64
}

func NewCache(config CacheConfig) *Cache {
	sr := &Cache{
		segments:           make([]*shard.Shard, config.NumberOfSegments),
		options:            config,
		updatedEntriesChan: make(chan *storage.CacheEntry, config.NumberOfSegments*1024),
		Storage:            file_storage.NewFileCacheStorage(config.FilePath, config.MaxFileSize, time.Second*5),
	}

	sr.initFromStorage()

	go sr.setupUpdateProxy()

	return sr
}

func (c *Cache) setupUpdateProxy() {

	for {
		update := <-c.updatedEntriesChan

		c.Storage.OnEntryUpdated(update)
	}

}

func (c *Cache) initFromStorage() {
	entries, err := c.Storage.ParseAllEntries()

	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		c.set(entry.Key, entry.Value, entry.Expiration)
	}
}

func (c *Cache) hashOfKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	hash32 := int(h.Sum32())
	return hash32 % c.options.NumberOfSegments
}

func (c *Cache) set(key string, value []byte, expiration time.Time) error {

	hashOfKey := c.hashOfKey(key)

	sh := c.segments[hashOfKey]

	if sh == nil {

		seg, err := shard.NewShard(c.options.InitialSegmentSize, c.options.SegmentWidth, c.updatedEntriesChan)

		if err != nil {
			return err
		}

		fmt.Printf("creating a new shard for width %v \n", c.options.SegmentWidth)

		c.segments[hashOfKey] = seg
		sh = seg
	}

	sh.Add(key, value, expiration)

	return nil
}

func (c *Cache) SetWithExpiration(key string, value []byte, duration time.Duration) error {

	err := c.set(key, value, time.Now().Add(duration))

	c.Storage.OnEntryUpdated(&storage.CacheEntry{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(duration),
		Deleted:    false,
	})

	return err
}

func (c *Cache) Set(key string, value []byte) error {
	return c.SetWithExpiration(key, value, time.Hour*1024*24)
}

func (c *Cache) Get(key string) []byte {
	value, _ := c.getWithDetail(key)
	return value
}

func (c *Cache) getWithDetail(key string) (value []byte, shardIndex int) {
	shardIndex = c.hashOfKey(key)
	shard := c.segments[shardIndex]

	if shard == nil {
		value = nil
		return
	}

	value = shard.Get(key)
	return
}

func (c *Cache) Delete(key string) {
	shardIndex := c.hashOfKey(key)
	shard := c.segments[shardIndex]
	shard.Delete(key)
}
