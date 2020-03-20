package storage

// CacheStorage defines the interfaces for the cache to persist data
type CacheStorage interface {

	// this method will read all the entries from file
	ParseAllEntries() []*CacheEntry

	// this method will be called once an entry has been updated
	OnEntryUpdated(entry *CacheEntry)
}
