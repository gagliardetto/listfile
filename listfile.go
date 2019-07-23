package listfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
)

type ListFile struct {
	file     *os.File
	mu       *sync.RWMutex
	isClosed bool
}

// Append appends one or more strings adding a newline to each.
func (lf *ListFile) Append(items ...string) error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

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

// Append appends one or more strings adding a newline to each.
func (lf *ListFile) UniqueAppend(items ...string) (int, error) {
	if lf.IsClosed() {
		return 0, errors.New("file is already closed")
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	var successfullyAdded int
	for _, item := range items {
		if lf.noMutexHasStringLine(item) {
			continue
		}
		_, err := lf.file.WriteString(item + "\n")
		if err != nil {
			return err
		}
		successfullyAdded++
	}
	return successfullyAdded, nil
}

func (lf *ListFile) HasStringLine(s string) bool {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}
	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexHasStringLine(s)
}

func (lf *ListFile) noMutexHasStringLine(s string) bool {
	var has bool
	err := lf.noMutexIterateLines(textScanner(func(line string) bool {
		if s == line {
			has = true
			return false
		}
		return true
	}))
	if err != nil {
		// TODO: return an error?
		panic(err)
	}
	return has
}
func (lf *ListFile) HasBytesLine(b []byte) bool {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}
	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexHasBytesLine(b)
}
func (lf *ListFile) noMutexHasBytesLine(b []byte) bool {
	err := lf.noMutexIterateLines(textScanner(func(line string) bool {
		if bytes.Equal(b, line) {
			has = true
			return false
		}
		return true
	}))
	if err != nil {
		// TODO: return an error?
		panic(err)
	}
	return has
}

func (lf *ListFile) IsClosed() bool {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	// if the file is already closed, any operation on it
	// will panic.
	return lf.isClosed
}

// Len returns the length in bytes of the file;
// WARNING: this operation does not lock the mutex.
func (lf *ListFile) Len() int {
	if lf.IsClosed() {
		return 0
	}

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

func (lf *ListFile) LenLines() int {
	if lf.IsClosed() {
		return 0
	}

	var count int
	lf.IterateLines(func(line string) bool {
		count++
		return true
	})
	return count
}

// IterateLines iterates on the lines of the list;
// this operation is LOCKING.
func (lf *ListFile) IterateLines(iterator func(line string) bool) error {
	return lf.iterateLines(textScanner(iterator))
}
func (lf *ListFile) IterateLinesAsBytes(iterator func(line []byte) bool) error {
	return lf.iterateLines(bytesScanner(iterator))
}
func textScanner(iterator func(line string) bool) func(scanner *bufio.Scanner) bool {
	return func(scanner *bufio.Scanner) bool {
		return iterator(scanner.Text())
	}
}
func bytesScanner(iterator func(line []byte) bool) func(scanner *bufio.Scanner) bool {
	return func(scanner *bufio.Scanner) bool {
		return iterator(scanner.Bytes())
	}
}
func (lf *ListFile) iterateLines(iterator func(scanner *bufio.Scanner) bool) error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexIterateLines(iterator)
}

func (lf *ListFile) noMutexIterateLines(iterator func(scanner *bufio.Scanner) bool) error {

	sectionReader := io.NewSectionReader(lf.file, 0, lf.LenInt64())

	scanner := bufio.NewScanner(sectionReader)
	for scanner.Scan() {
		doContinue := iterator(scanner)
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
	if lf.IsClosed() {
		return nil
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	err := lf.file.Sync()
	if err != nil {
		return err
	}

	err = lf.file.Close()
	if err != nil {
		return err
	}

	lf.isClosed = true

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
