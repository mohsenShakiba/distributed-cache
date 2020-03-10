package file_storage

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"../storage"
	"time"
)

type FileCacheStorage struct {
	Path string
	MaxFileSize int64
	FilePrefix string
}

func (fcs *FileCacheStorage) getAllFiles() ([]fileDescriptor, error) {
	f, err := os.Open(fcs.Path)

	if err != nil {
		return nil, err
	}

	list, err := f.Readdir(-1)

	if err != nil {
		return nil, err
	}

	fileList := make([]fileDescriptor, 1)
	for _, fileInfo := range list {

		if fileInfo.IsDir() {
			continue
		}

		if !strings.Contains(fileInfo.Name(), fcs.FilePrefix) {
			continue
		}

		// read all the cont
		file, err :=  os.Open(fcs.Path + "/" + fileInfo.Name())

		if err != nil {
			return nil, err
		}

		fDescriptor := fileDescriptor{file:file, fileInfo: fileInfo}

		fileList = append(fileList, fDescriptor)
	}

	// sort the files on date desc
	sort.SliceStable(fileList, func (i, j int ) bool {
		return fileList[i].order() < fileList[j].order()
	})

	return fileList, nil
}

func (fcs *FileCacheStorage) parseAllEntries() ([]*storage.CacheEntry, error) {

	fDescriptors, err := fcs.getAllFiles()

	// close the files
	defer func () {
		for _, f := range fDescriptors {
			f.file.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	entries := make([]*storage.CacheEntry, 1000)

	for _, fDescriptor := range fDescriptors {

		buf := new(bytes.Buffer)
		buf.ReadFrom(fDescriptor.file)
		contents := buf.Bytes()

		var fileEntries []*storage.CacheEntry

		if err := json.Unmarshal(contents, &fileEntries); err != nil {
			return nil, err
		}

		entries = append(entries, fileEntries...)
	}

	return entries, nil
}

func (fcs *FileCacheStorage) writeAllEntries(entries []*storage.CacheEntry) {
	file, err := fcs.getAvailableFile()

	if err != nil {
		fmt.Printf("counn't create new file due to %v", err)
		return
	}

	defer file.Close()

	marshalledEntries, err := json.Marshal(entries)

	file.Write(marshalledEntries)
}

func (fcs *FileCacheStorage) getAvailableFile() (*os.File, error) {

	// get all files
	fDescriptors, err := fcs.getAllFiles()

	// if error
	if err != nil {
		return nil, err
	}

	// check if file has enough space
	if len(fDescriptors) == 0 {
		return fcs.createNewFile()
	}

	lastFDescirptor := fDescriptors[len(fDescriptors) - 1]

	if lastFDescirptor.hasReachedMaxSize(fcs.MaxFileSize) {
		return fcs.createNewFile()
	}

	return lastFDescirptor.file, nil
}

func (fcs *FileCacheStorage) createNewFile() (*os.File, error) {
	return os.Create(fcs.Path + "/" + strconv.FormatInt(time.Now().Unix(), 10))
}

func (fcs *FileCacheStorage) compressFiles() {

}