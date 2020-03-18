package cache

import (
	"fmt"
	"hash/fnv"
	"time"

	"github.com/mohsenShakiba/distributed-cache/file_storage"
	"github.com/mohsenShakiba/distributed-cache/shard"
	"github.com/mohsenShakiba/distributed-cache/storage"
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
		c.set(entry.Key, entry.Value)
	}
}

func (c *Cache) hashOfKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	hash32 := int(h.Sum32())
	return hash32 % c.options.NumberOfSegments
}

func (c *Cache) set(key string, value []byte) error {

	hashOfKey := c.hashOfKey(key)

	shard := c.segments[hashOfKey]

	if shard == nil {
		seg, err := shard.NewShard(c.options.InitialSegmentSize, c.options.SegmentWidth, c.updatedEntriesChan)
		if err != nil {
			return err
		}

		fmt.Printf("creating a new shard for width %v \n", c.options.SegmentWidth)

		c.segments[hashOfKey] = seg
		shard = seg
	}

	shard.Add(key, value)

	return nil
}

func (c *Cache) Set(key string, value []byte) error {

	err := c.set(key, value)

	c.Storage.OnEntryUpdated(&storage.CacheEntry{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(time.Second * 10),
		Deleted:    false,
	})

	return err
}

func (c *Cache) Get(key string) []byte {

	hashOfKey := c.hashOfKey(key)

	segment := c.segments[hashOfKey]

	if segment == nil {
		return nil
	}

	return segment.Get(key)
}
