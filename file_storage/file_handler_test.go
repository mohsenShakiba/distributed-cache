package file_storage

import (
	"strings"
	"testing"
)

func TestCreatingFile(t *testing.T) {
	fh := newFileHandler("./", "test_", 128)

	defer fh.clear()

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

	fh.write([]byte("0"))

	if len(fh.listOfAllFiles) != 2 {
		t.Error("the first file hasn't been switched yet")
	}
}

func TestReadingFile(t *testing.T) {

}

func TestSortingFiles(t *testing.T) {

}
