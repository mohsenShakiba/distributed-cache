package storage

import "time"

type CacheEntry struct {
	Key string
	Value interface{}
	Expiration time.Time
	Deleted bool
}
