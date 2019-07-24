package listfile

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/petar/GoLLRB/llrb"
)

type ListFile struct {
	file     *os.File
	mu       *sync.RWMutex
	isClosed bool
	tree     *llrb.LLRB
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
		// add to tree to be able then to search it:
		// TODO: use InsertNoReplace instead?
		lf.tree.ReplaceOrInsert(Item(item))
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

	var successfullyAddedItemsCount int
	for _, item := range items {
		has := lf.noMutexHasStringLine(item)
		if has {
			continue
		}
		_, err := lf.file.WriteString(item + "\n")
		if err != nil {
			return successfullyAddedItemsCount, err
		}
		// add to tree to be able then to search it:
		lf.tree.ReplaceOrInsert(Item(item))
		successfullyAddedItemsCount++
	}
	return successfullyAddedItemsCount, nil
}

func (lf *ListFile) HasStringLine(s string) (bool, error) {
	if lf.IsClosed() {
		return false, errors.New("file is already closed")
	}
	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexHasStringLine(s), nil
}

func (lf *ListFile) noMutexHasStringLine(s string) bool {
	return lf.tree.Has(Item(s))
}
func (lf *ListFile) HasBytesLine(b []byte) (bool, error) {
	if lf.IsClosed() {
		return false, errors.New("file is already closed")
	}
	// TODO: use a Lock() ar a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexHasBytesLine(b), nil
}
func (lf *ListFile) noMutexHasBytesLine(b []byte) bool {
	return lf.noMutexHasStringLine(string(b))
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
	// NOTE: here we don't use IsClosed() because
	// it uses the mutex; Len() is used in noMutexIterateLines
	// which is called after another mutex is locked,
	// making IsClosed() wait forever for the mutex unlock.
	if lf.isClosed {
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
		tree: llrb.New(),
	}

	err = lf.loadExistingToTree()
	if err != nil {
		return nil, fmt.Errorf("error while loadExistingToTree: %s", err)
	}

	return lf, nil
}
func (lf *ListFile) loadExistingToTree() error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	return lf.IterateLines(func(line string) bool {
		lf.tree.ReplaceOrInsert(Item(line))
		return true
	})
}

type Item string

func (oid Item) Less(than llrb.Item) bool {
	return oid < than.(Item)
}
