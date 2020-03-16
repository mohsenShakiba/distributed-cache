package storage

import (
	"encoding/json"
	"time"
)

// CacheEntry represents an entry to be stored in storage
type CacheEntry struct {
	Key        string
	Value      []byte
	Expiration time.Time
	Deleted    bool
}

// Serialize uses json format to serialize the entry
func (ce *CacheEntry) Serialize() ([]byte, error) {
	return json.Marshal(ce)
}

// Deserialize uses json format to deserialize entry
func (cs *CacheEntry) Deserialize(line []byte) error {
	return json.Unmarshal(line, cs)
}

func Unique(entries []*CacheEntry) []*CacheEntry {
	updatedEntriesSet := make(map[string]*CacheEntry, 0)

	for _, updatedEntry := range entries {
		updatedEntriesSet[updatedEntry.Key] = updatedEntry
	}

	updatedEntriesList := make([]*CacheEntry, 0, len(updatedEntriesSet))

	for _, value := range updatedEntriesSet {
		updatedEntriesList = append(updatedEntriesList, value)
	}

	return updatedEntriesList
}
