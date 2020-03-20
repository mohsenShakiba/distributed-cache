package cache

import (
	"os"
	"testing"
	"time"
)

func TestReadWriteDelete(t *testing.T) {
	td := os.TempDir()

	conf := CacheConfig{
		InitialSegmentSize: 1000,
		FilePath:           td,
		MaxFileSize:        128,
		NumberOfSegments:   2,
		SegmentWidth:       128,
		SyncDuration:       time.Second,
	}

	cache := NewCache(conf)

	cache.Set("1", []byte("1"))
	cache.Set("2", []byte("2"))

	value1, shardIndex1 := cache.getWithDetail("1")
	value2, shardIndex2 := cache.getWithDetail("2")

	// making sure the shard index distribution was ok
	if shardIndex1 == shardIndex2 {
		t.Errorf("the returned shard index %v must be different", shardIndex1)
	}

	// making sure the returned value was valid
	if string(value1) != "1" {
		t.Errorf("the returned value for key 1 was %v", string(value1))
	}

	if string(value2) != "2" {
		t.Errorf("the returned value for key 2 was %v", string(value2))
	}

	// updating entries
	cache.Set("1", []byte("-1"))

	value1, _ = cache.getWithDetail("1")

	// making sure the updated values are correct
	if string(value1) != "-1" {
		t.Errorf("the returned value for key 1 was %v", string(value1))
	}

	// deleting entries
	cache.Delete("1")

	value1, _ = cache.getWithDetail("1")

	if value1 != nil {
		t.Error("the entry wasn't deleted")
	}

	// testing expiration
	cache.SetWithExpiration("1", []byte("1"), time.Second*1)

	// making sure the entry isn't returned after 1 second
	time.Sleep(time.Second)

	value1 = cache.Get("1")

	if value1 != nil {
		t.Error("the entry wasn't expired")
	}

}

func TestInitFromStorage(t *testing.T) {
	td := os.TempDir()

	conf := CacheConfig{
		InitialSegmentSize: 1000,
		FilePath:           td,
		MaxFileSize:        128,
		NumberOfSegments:   2,
		SegmentWidth:       128,
		SyncDuration:       time.Second,
	}

	cache := NewCache(conf)

	err := cache.Set("1", []byte("1"))

	if err != nil {
		t.Error(err)
	}

	cache.SetWithExpiration("2", []byte("2"), time.Second)

	cache.Set("3", []byte("3"))

	cache.Delete("3")

	time.Sleep(time.Second)

	cache = NewCache(conf)

	res1 := cache.Get("1")

	if string(res1) != "1" {
		t.Error("the returned value is incorrect", string(res1))
	}

	res2 := cache.Get("2")

	if res2 != nil {
		t.Error("the entry wasn't expired")
	}

	res3 := cache.Get("3")

	if res3 != nil {
		t.Error("the entry wasn't deleted")
	}

}
