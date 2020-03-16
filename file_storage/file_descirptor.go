package file_storage

import (
	"os"
	"strconv"
)

type fileDescriptor struct {
	file     *os.File
	fileInfo os.FileInfo
}

func (fd fileDescriptor) hasReachedMaxSize(maxSize int64) bool {
	return fd.fileInfo.Size() > maxSize
}

func (fd fileDescriptor) order() int {
	order, err := strconv.Atoi(fd.fileInfo.Name())
	if err != nil {
		return 0
	}
	return order
}
