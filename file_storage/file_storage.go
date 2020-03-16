package file_storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/mohsenShakiba/distributed-cache/storage"
	"time"
)

type FileCacheStorage struct {
	FlushInterval        int
	fh                   *fileHandler
	listOfUpdatedEntries []*storage.CacheEntry
}

func NewFileCacheStorage(path string, maxFileSize int64, flushDuration time.Duration) *FileCacheStorage {

	fileCacheStorage := &FileCacheStorage{
		fh:                   newFileHandler(path, "temp_", maxFileSize),
		listOfUpdatedEntries: make([]*storage.CacheEntry, 0, 1000),
	}

	go fileCacheStorage.setupFlushTimer(flushDuration)

	return fileCacheStorage
}

func (fcs *FileCacheStorage) OnEntryUpdated(entry *storage.CacheEntry) {
	fcs.listOfUpdatedEntries = append(fcs.listOfUpdatedEntries, entry)
}

func (fcs *FileCacheStorage) ParseAllEntries() ([]*storage.CacheEntry, error) {

	fDescriptors := fcs.fh.listOfAllFiles

	entries := make([]*storage.CacheEntry, 0, 1000)

	for _, fDescriptor := range fDescriptors {

		scanner := bufio.NewScanner(fDescriptor.file)
		for scanner.Scan() {
			entry := &storage.CacheEntry{}
			text := scanner.Text()
			if err := json.Unmarshal([]byte(text), entry); err != nil {
				return nil, err
			}

			entries = append(entries, entry)
		}
	}

	return entries, nil
}

func (fcs *FileCacheStorage) setupFlushTimer(flushDuration time.Duration) {
	ticker := time.NewTicker(flushDuration)
	for {
		<-ticker.C
		fcs.flushUpdatedEntries()
	}
}

func (fcs *FileCacheStorage) compressFiles() {

}

func (fcs *FileCacheStorage) flushUpdatedEntries() {

	updatedEntries := make(map[string]*storage.CacheEntry, 0)

	for _, updatedEntry := range fcs.listOfUpdatedEntries {
		updatedEntries[updatedEntry.Key] = updatedEntry
	}

	for _, value := range updatedEntries {
		data, err := value.Serialize()

		if err != nil {
			fmt.Println("error while marshalling the entry", err)
		}

		fcs.fh.write(data)
	}

	fcs.listOfUpdatedEntries = make([]*storage.CacheEntry, 0, len(fcs.listOfUpdatedEntries))

}
