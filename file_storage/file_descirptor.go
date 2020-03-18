package file_storage

import (
	"os"
	"strconv"
)

type fileDescriptor struct {
	file        *os.File
	fileInfo    os.FileInfo
	currentSize int64
}

func newFileDescriptor(file *os.File) *fileDescriptor {

	fi, _ := file.Stat()

	return &fileDescriptor{
		file:        file,
		fileInfo:    fi,
		currentSize: fi.Size(),
	}
}

func (fd *fileDescriptor) hasReachedMaxSize(maxSize int64) bool {
	return fd.currentSize >= maxSize
}

func (fd *fileDescriptor) order() int {
	order, err := strconv.Atoi(fd.fileInfo.Name())
	if err != nil {
		return 0
	}
	return order
}

func (fd *fileDescriptor) write(data []byte) {
	bytesWritten, _ := fd.file.Write(data)
	fd.currentSize += int64(bytesWritten)
}
