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
