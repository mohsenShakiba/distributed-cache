package file_storage

import (
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type fileHandler struct {
	path               string
	prefix             string
	maxFileSize        int64
	listOfAllFiles     []fileDescriptor
	currentWorkingFile fileDescriptor
}

func newFileHandler(path string, prefix string, maxFileSize int64) *fileHandler {
	fh := &fileHandler{
		path:        path,
		prefix:      prefix,
		maxFileSize: maxFileSize,
	}

	fh.init()

	return fh
}

// init will initialize all files and current file fields
func (fh *fileHandler) init() {

	if fh.listOfAllFiles == nil {

		f, err := os.Open(fh.path)

		if err != nil {
			panic(err)
		}

		list, err := f.Readdir(-1)

		if err != nil {
			panic(err)
		}

		fileList := make([]fileDescriptor, 0, 100)
		for _, fileInfo := range list {

			if fileInfo.IsDir() {
				continue
			}

			if !strings.Contains(fileInfo.Name(), fh.prefix) {
				continue
			}

			// read all the content
			file, err := os.OpenFile(fh.path+"/"+fileInfo.Name(), os.O_APPEND|os.O_CREATE, 0600)

			if err != nil {
				panic(err)
			}

			fDescriptor := fileDescriptor{file: file, fileInfo: fileInfo}

			fmt.Println("adding file with name", fileInfo.Name())

			fileList = append(fileList, fDescriptor)
		}

		// sort the files on date desc
		sort.SliceStable(fileList, func(i, j int) bool {
			return fileList[i].order() < fileList[j].order()
		})

		fh.listOfAllFiles = fileList
	}

	// if first time, setup new file
	if len(fh.listOfAllFiles) == 0 || fh.listOfAllFiles[len(fh.listOfAllFiles)-1].hasReachedMaxSize(fh.maxFileSize) {
		f, err := os.OpenFile(fh.path+"/"+fh.prefix+strconv.FormatInt(time.Now().Unix(), 10), os.O_APPEND|os.O_CREATE, 0600)

		if err != nil {
			panic(err)
		}

		fi, err := f.Stat()

		if err != nil {
			panic(err)
		}

		fh.listOfAllFiles = append(fh.listOfAllFiles, fileDescriptor{file: f, fileInfo: fi})
	}

	fh.currentWorkingFile = fh.listOfAllFiles[len(fh.listOfAllFiles)-1]

}

func (fh *fileHandler) write(content []byte) {
	fh.init()

	_, err := fh.currentWorkingFile.file.Write(content)
	_, err = fh.currentWorkingFile.file.Write([]byte("\n"))

	if err != nil {
		fmt.Println("error while trying to write to file", err)
	}

}

func (fh *fileHandler) clear() {
	for _, i := range fh.listOfAllFiles {
		fmt.Println("removing file", i.fileInfo.Name())
		os.Remove(fh.path + "/" + i.fileInfo.Name())
	}
}
