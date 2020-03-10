package storage


type CacheStorage interface {

	WriteRange(cacheEntries []*CacheEntry)
	ReadAll() []*CacheEntry

}