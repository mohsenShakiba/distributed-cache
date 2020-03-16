package shard

import (
	"../file_storage"
	"../storage"
	"fmt"
	"hash/fnv"
	"time"
)

type ShardRing struct {
	segments           []*Shard
	options            ShardRingConfig
	updatedEntriesChan chan EntryStatus
	Storage            *file_storage.FileCacheStorage
}

type ShardRingConfig struct {
	InitialSegmentSize int64
	NumberOfSegments   int
	SegmentWidth       int
	SyncDuration       time.Duration
	FilePath           string
	MaxFileSize        int64
}

func CreateNewSegmentPool(config ShardRingConfig) *ShardRing {
	sr := &ShardRing{
		segments:           make([]*Shard, config.NumberOfSegments),
		options:            config,
		updatedEntriesChan: make(chan EntryStatus, config.NumberOfSegments*1024),
		Storage:            file_storage.NewFileCacheStorage(config.FilePath, config.MaxFileSize, time.Second*5),
	}

	sr.initFromStorage()

	return sr
}

func (sr *ShardRing) initFromStorage() {
	entries, err := sr.Storage.ParseAllEntries()

	if err != nil {
		panic(err)
	}

	for _, entry := range entries {
		sr.set(entry.Key, entry.Value)
	}
}

func (sr *ShardRing) hashOfKey(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	hash32 := int(h.Sum32())
	return hash32 % sr.options.NumberOfSegments
}

func (sr *ShardRing) set(key string, value []byte) error {

	hashOfKey := sr.hashOfKey(key)

	shard := sr.segments[hashOfKey]

	if shard == nil {
		seg, err := createNewShard(sr.options.InitialSegmentSize, sr.options.SegmentWidth, sr.updatedEntriesChan)
		if err != nil {
			return err
		}

		fmt.Printf("creating a new shard for width %v \n", sr.options.SegmentWidth)

		sr.segments[hashOfKey] = seg
		shard = seg
	}

	shard.Add(key, value)

	return nil
}

func (sr *ShardRing) Set(key string, value []byte) error {

	err := sr.set(key, value)

	sr.Storage.OnEntryUpdated(&storage.CacheEntry{
		Key:        key,
		Value:      value,
		Expiration: time.Now().Add(time.Second * 10),
		Deleted:    false,
	})

	return err
}

func (sr *ShardRing) Get(key string) []byte {

	hashOfKey := sr.hashOfKey(key)

	segment := sr.segments[hashOfKey]

	if segment == nil {
		return nil
	}

	return segment.Get(key)
}

func (sr *ShardRing) LogHits() {
	for _, s := range sr.segments {
		fmt.Printf("took %v hits \n", s.hits)
	}
}
