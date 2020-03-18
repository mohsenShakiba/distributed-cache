package file_storage

import (
	"io/ioutil"
	"strings"
	"testing"
	"time"
)

func TestCreatingFile(t *testing.T) {

	td, err := ioutil.TempDir("", "")

	if err != nil {
		t.Error(err)
	}

	fh := newFileHandler(td, "test_", 128)

	if len(fh.listOfAllFiles) != 1 {
		t.Error("the file handler didn't create valid files", len(fh.listOfAllFiles))
	}

	if fh.currentWorkingFile.file == nil {
		t.Error("the file handler didn't create working file")
	}

	if !strings.Contains(fh.currentWorkingFile.file.Name(), fh.prefix) {
		t.Error("the file handler didn't create the name of the file correctly", fh.currentWorkingFile.file.Name(), fh.prefix)
	}

	content := make([]byte, 128)
	fh.write(content)

	if len(fh.listOfAllFiles) != 1 {
		t.Error("the first file shouldn't be switched yet")
	}

	time.Sleep(time.Second * 1)

	fh.write([]byte("0"))

	if len(fh.listOfAllFiles) != 2 {
		t.Error("the first file hasn't been switched yet")
	}
}

func TestReadingFile(t *testing.T) {

	// create a file storage
	td, err := ioutil.TempDir("", "")

	if err != nil {
		t.Error(err)
	}

	fh := newFileHandler(td, "test_", 128)

	// write to first file
	fh.write([]byte("1"))
	fh.write(make([]byte, 125))

	time.Sleep(time.Second)

	// write to second file
	fh.write([]byte("2"))

	// get back the result
	fh2 := newFileHandler(td, "test_", 128)
	list := fh2.listOfAllFiles

	if len(list) != 2 {
		t.Error("number of files is invalid", len(list))
	}

	firstByte := make([]byte, 1)
	secondByte := make([]byte, 1)
	list[0].file.Read(firstByte)
	list[1].file.Read(secondByte)

	if string(firstByte[:1]) != "1" {
		t.Error("the first result is invalid", string(firstByte[:1]))
	}

	if string(secondByte[:1]) != "2" {
		t.Error("the second result is invalid", string(secondByte[:1]))
	}

}
