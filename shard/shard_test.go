package shard

import (
	"testing"

	"github.com/mohsenShakiba/distributed-cache/storage"
)

func TestAddingAndGetingItems(t *testing.T) {
	shard, err := NewShard(100, 128, make(chan<- *storage.CacheEntry, 100))

	if err != nil {
		t.Error(err)
	}

	shard.Add("test", []byte("1"))

	res := shard.Get("test")

	if string(res) != "1" {
		t.Error("the expected value was not returned")
	}

	shard.Add("test", []byte("2"))

	res2 := shard.Get("test")

	if string(res2) != "2" {
		t.Error("the expected value was not returned")
	}

	shard.Delete("test")

	res3 := shard.Get("test")

	if res3 != nil {
		t.Error("the expected value was not returned")
	}
}

func TestUpdatedItemsChan(t *testing.T) {

	ch := make(chan *storage.CacheEntry, 100)

	shard, err := NewShard(100, 128, ch)

	if err != nil {
		t.Error(err)
	}

	shard.Add("test", []byte("1"))
	shard.Add("test", []byte("2"))

	shard.Delete("test")

	ue1 := <-ch
	ue2 := <-ch
	ue3 := <-ch

	if string(ue1.Value) != "1" || ue1.Key != "test" || ue1.Deleted == true {
		t.Error("wrong updated entry was generated")
	}

	if string(ue2.Value) != "2" || ue2.Key != "test" || ue2.Deleted == true {
		t.Error("wrong updated entry was generated")
	}

	if ue3.Key != "test" || ue3.Deleted != true {
		t.Error("wrong updated entry was generated")
	}

}

func TestGettingAllItems(t *testing.T) {
	shard, err := NewShard(100, 128, make(chan<- *storage.CacheEntry, 100))

	if err != nil {
		t.Error(err)
	}

	shard.Add("test", []byte("1"))

	items := shard.getAllCacheEntries()

	if len(items) != 1 {
		t.Error("invalid number of items")
	}

	entry0 := items[0]

	if string(entry0.Value) != "1" || entry0.Key != "test" || entry0.Deleted == true {
		t.Error("wrong entry was given")
	}

	shard.Add("test", []byte("2"))

	items = shard.getAllCacheEntries()

	if len(items) != 1 {
		t.Error("invalid number of items")
	}

	entry0 = items[0]

	if string(entry0.Value) != "2" || entry0.Key != "test" || entry0.Deleted == true {
		t.Error("wrong entry was given")
	}

	shard.Add("test2", []byte("1"))

	items = shard.getAllCacheEntries()

	if len(items) != 2 {
		t.Error("invalid number of items")
	}

	shard.Delete("test")
	shard.Delete("test2")

	items = shard.getAllCacheEntries()

	if len(items) != 0 {
		t.Error("invalid number of items")
	}
}
