package file_storage

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/mohsenShakiba/distributed-cache/pkg/storage"
)

type FileCacheStorage struct {
	FlushInterval        int
	fh                   *fileHandler
	listOfUpdatedEntries []*storage.CacheEntry
	mutex                sync.Mutex
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

	return storage.Unique(entries), nil
}

func (fcs *FileCacheStorage) setupFlushTimer(flushDuration time.Duration) {
	ticker := time.NewTicker(flushDuration)
	for {
		<-ticker.C
		fcs.flushUpdatedEntries()
	}
}

func (fcs *FileCacheStorage) ClearAndWriteAll(allEntries []*storage.CacheEntry) {

	fcs.mutex.Lock()

	defer fcs.mutex.Unlock()

	// remove all the previous files
	for _, f := range fcs.fh.listOfAllFiles {
		os.Remove(f.file.Name())
	}

	// call init to create new file
	for _, entry := range allEntries {
		data, _ := entry.Serialize()
		fcs.fh.write(data)
	}

}

func (fcs *FileCacheStorage) flushUpdatedEntries() {

	fcs.mutex.Lock()
	defer fcs.mutex.Unlock()

	updatedEntries := storage.Unique(fcs.listOfUpdatedEntries)

	for _, value := range updatedEntries {
		data, err := value.Serialize()

		if err != nil {
			fmt.Println("error while marshalling the entry", err)
		}

		fcs.fh.write(data)
	}

	fcs.listOfUpdatedEntries = make([]*storage.CacheEntry, 0, len(fcs.listOfUpdatedEntries))

}
