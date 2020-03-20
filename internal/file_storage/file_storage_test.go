package file_storage

import (
	"io/ioutil"
	"strconv"
	"testing"
	"time"

	"github.com/mohsenShakiba/distributed-cache/storage"
)

func TestWritingAndReadingSameKeyFileStorage(t *testing.T) {

	td, err := ioutil.TempDir("", "")

	if err != nil {
		t.Error(err)
	}

	// create storage
	strg := NewFileCacheStorage(td, 128, time.Second)

	strg.OnEntryUpdated(&storage.CacheEntry{
		Key:        "1",
		Value:      []byte("1"),
		Expiration: time.Now().Add(time.Second),
		Deleted:    false,
	})

	strg.OnEntryUpdated(&storage.CacheEntry{
		Key:        "1",
		Value:      []byte("2"),
		Expiration: time.Now().Add(time.Second),
		Deleted:    false,
	})

	time.Sleep(2 * time.Second)

	// recreate the storage

	strg = NewFileCacheStorage(td, 128, time.Second)

	entries, _ := strg.ParseAllEntries()

	if len(entries) != 1 {
		t.Error("the returned entries are invalid")
	}

	e := entries[0]
	if string(e.Value) != "2" {
		t.Error("the value of entry is invalid")
	}
}

func TestWritingAndReadingSequentialFileStorage(t *testing.T) {

	td, err := ioutil.TempDir("", "")

	if err != nil {
		t.Error(err)
	}

	// create storage
	strg := NewFileCacheStorage(td, 8096, time.Second)

	count := 10

	for i := 0; i < count; i++ {
		strg.OnEntryUpdated(&storage.CacheEntry{
			Key:        strconv.Itoa(i),
			Value:      []byte(strconv.Itoa(i)),
			Expiration: time.Now().Add(time.Second),
			Deleted:    false,
		})
	}

	for i := 0; i < count; i++ {
		strg.OnEntryUpdated(&storage.CacheEntry{
			Key:        strconv.Itoa(i),
			Value:      []byte(strconv.Itoa(i)),
			Expiration: time.Now().Add(time.Second),
			Deleted:    true,
		})
	}

	time.Sleep(2 * time.Second)

	// recreate the storage

	strg = NewFileCacheStorage(td, 8096, time.Second)

	entries, _ := strg.ParseAllEntries()

	if len(entries) != count {
		t.Error("the returned entries are invalid", len(entries))
	}

	for _, entry := range entries {
		if string(entry.Value) != entry.Key {
			t.Error("value don't math between", string(entry.Value), entry.Key)
		}
	}
}
