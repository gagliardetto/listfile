package listfile

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sync"
)

type ListFile struct {
	file *os.File
	mu   *sync.RWMutex
}

// Append appends one or more strings adding a newline to each.
func (lf *ListFile) Append(items ...string) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()

	for _, item := range items {
		_, err := lf.file.WriteString(item + "\n")
		if err != nil {
			return err
		}
	}
	return nil
}

// Len returns the length in bytes of the file;
// WARNING: this operation does not lock the mutex.
func (lf *ListFile) Len() int {
	err := lf.file.Sync()
	if err != nil {
		// TODO: not panic??
		panic(err)
	}

	info, err := lf.file.Stat()
	if err != nil {
		// TODO: not panic??
		panic(err)
	}

	return int(info.Size())
}
func (lf *ListFile) LenInt64() int64 {
	return int64(lf.Len())
}

// IterateLines iterates on the lines of the list;
// this operation is LOCKING.
func (lf *ListFile) IterateLines(iterator func(line string) bool) error {
	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	sectionReader := io.NewSectionReader(lf.file, 0, lf.LenInt64())

	scanner := bufio.NewScanner(sectionReader)
	for scanner.Scan() {
		doContinue := iterator(scanner.Text())
		if !doContinue {
			return nil
		}
		err := scanner.Err()
		if err != nil {
			return fmt.Errorf("error while iterating over scanner: %s", err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error of scanner: %s", err)
	}

	return nil
}

func (lf *ListFile) Close() error {
	// TODO: lock the mutex?

	err := lf.file.Sync()
	if err != nil {
		return err
	}

	err = lf.file.Close()
	if err != nil {
		return err
	}

	return nil
}

func New(filepath string) (*ListFile, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error while OpenFile: %s", err)
	}

	lf := &ListFile{
		file: file,
		mu:   &sync.RWMutex{},
	}

	return lf, nil
}
