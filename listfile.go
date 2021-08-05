package listfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"

	"github.com/gagliardetto/hashsearch"
)

type ListFile struct {
	file             *os.File
	mu               *sync.RWMutex
	isClosed         bool
	mainIndex        *hashsearch.HashSearch
	secondaryIndexes map[string]*Index
}

// New initializes a new ListFile from the provided file;
// NOTE: this is *without* an index.
func New(filepath string) (*ListFile, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error while OpenFile: %s", err)
	}

	lf := &ListFile{
		file: file,
		mu:   &sync.RWMutex{},
		// Allow to create custom indexes:
		secondaryIndexes: make(map[string]*Index),
	}

	return lf, nil
}

// NewIndexed initializes a new ListFile from the provided file
// along with an index of the lines.
func NewIndexed(filepath string) (*ListFile, error) {
	file, err := os.OpenFile(filepath, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("error while OpenFile: %s", err)
	}

	lf := &ListFile{
		file: file,
		mu:   &sync.RWMutex{},
		// Main index for the lines of the file:
		mainIndex: hashsearch.New(),
		// Allow to create custom indexes:
		secondaryIndexes: make(map[string]*Index),
	}

	err = lf.loadExistingToMainIndex()
	if err != nil {
		return nil, fmt.Errorf("error while loadExistingToMainIndex: %s", err)
	}

	return lf, nil
}

// loadExistingToMainIndex indexes the current contents of the file (line by line).
func (lf *ListFile) loadExistingToMainIndex() error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	lf.mu.Lock()
	defer func() {
		lf.mainIndex.Sort()
		lf.mu.Unlock()
	}()

	return lf.noMutexIterateLines(func(line []byte) bool {
		lf.mainIndex.WARNING_UnsortedAppendBytes(line)
		return true
	})
}

func (lf *ListFile) noMutexIterateLines(iterator func(b []byte) bool) error {
	sectionReader := io.NewSectionReader(lf.file, 0, lf.NumBytes())

	reader := bufio.NewReader(sectionReader)
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				return fmt.Errorf("error of reader: %s", err)
			}
			break
		}
		// remove final newspace:
		line = bytes.TrimSuffix(line, []byte("\n"))
		doContinue := iterator(line)
		if !doContinue {
			return nil
		}
	}

	return nil
}

// readln returns a single line (without the ending \n)
// from the input buffered reader.
// An error is returned iff there is an error with the
// buffered reader.
func readln(r *bufio.Reader) ([]byte, error) {
	var (
		isPrefix bool  = true
		err      error = nil
		line, ln []byte
	)
	for isPrefix && err == nil {
		line, isPrefix, err = r.ReadLine()
		ln = append(ln, line...)
		//ln = make([]byte, len(line))
		//copy(ln, line)
	}
	return ln, err
}

// Append appends one or more strings adding a final newline to each.
func (lf *ListFile) Append(items ...string) error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	{
		// Gather all lines into a buffer:
		buf := new(bytes.Buffer)
		for _, item := range items {
			_, err := buf.WriteString(item + "\n")
			if err != nil {
				return err
			}
		}
		// Write the buffer to the file (in one file write):
		_, err := lf.file.Write(buf.Bytes())
		if err != nil {
			return err
		}
	}

	for _, item := range items {
		if lf.mainIndex != nil {
			// add to tree to be able then to search it:
			lf.mainIndex.Add(item)
		}
		// add to all indexes:
		for _, index := range lf.secondaryIndexes {
			index.backfillFunc([]byte(item))
		}
	}
	return nil
}

// Append appends one or more strings adding a newline to each.
func (lf *ListFile) AppendBytes(items ...[]byte) error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	{
		// Gather all lines into a buffer:
		buf := new(bytes.Buffer)
		for _, item := range items {
			_, err := buf.Write(append(item, []byte("\n")...))
			if err != nil {
				return err
			}
		}
		// Write the buffer to the file (in one file write):
		_, err := lf.file.Write(buf.Bytes())
		if err != nil {
			return err
		}
	}

	for _, item := range items {
		if lf.mainIndex != nil {
			// add to tree to be able then to search it:
			lf.mainIndex.AddFromBytes(item)
		}

		// add to all indexes:
		for _, index := range lf.secondaryIndexes {
			index.backfillFunc(item)
		}
	}
	return nil
}

// UniqueAppend appends one or more strings to the list;
// WARNING: onwly works with listfiles created with NewIndexed
func (lf *ListFile) UniqueAppend(items ...string) (int, error) {
	if lf.IsClosed() {
		return 0, errors.New("file is already closed")
	}

	if lf.mainIndex == nil {
		return 0, errors.New("listfile was created without root index")
	}

	lf.mu.Lock()
	defer lf.mu.Unlock()

	var successfullyAddedItemsCount int
	{
		buf := new(bytes.Buffer)
		for _, item := range items {
			had := lf.mainIndex.HasOrAdd(item)
			if had {
				continue
			}
			_, err := buf.WriteString(item + "\n")
			if err != nil {
				return successfullyAddedItemsCount, err
			}
			successfullyAddedItemsCount++

			// add to all indexes:
			for _, index := range lf.secondaryIndexes {
				index.backfillFunc([]byte(item))
			}
		}
		// Write the buffer to the file (in one file write):
		_, err := lf.file.Write(buf.Bytes())
		if err != nil {
			return successfullyAddedItemsCount, err
		}
	}
	return successfullyAddedItemsCount, nil
}

func (lf *ListFile) HasStringLine(s string) (bool, error) {
	//if lf.IsClosed() {
	//	return false, errors.New("file is already closed")
	//}
	// TODO: use a Lock() ar a RLock() ???

	if lf.mainIndex == nil {
		return false, errors.New("listfile was created without an index")
	}
	return lf.mainIndex.Has(s), nil
}

func (lf *ListFile) HasBytesLine(b []byte) (bool, error) {
	//if lf.IsClosed() {
	//	return false, errors.New("file is already closed")
	//}
	// TODO: use a Lock() ar a RLock() ???

	if lf.mainIndex == nil {
		return false, errors.New("listfile was created without an index")
	}
	return lf.mainIndex.HasBytes(b), nil
}

type Index struct {
	indexer      *hashsearch.Uint64
	backfillFunc func(line []byte) bool
}

// HasUint64ByIndex checks whether the index contains a specific value.
func (lf *ListFile) HasUint64ByIndex(indexName string, val uint64) bool {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	index, ok := lf.secondaryIndexes[indexName]
	if !ok {
		// TODO: if the index does not exist, return error, or create it, or just return false?
		return false
	}
	return index.indexer.Has(val)
}

func newBackfillFuncUint64(index *Index, uint64ColGetter func(item []byte) uint64) func(line []byte) bool {
	return func(line []byte) bool {
		colVal := uint64ColGetter(line)
		index.indexer.Add(colVal)
		return true
	}
}

// CreateIndexByUint64 creates a new index using the uint64 value returned by the uint64ColGetter
// When the index is created, a backfill is executed on all exisitng lines in the listfile.
// If the index already exists, nothing is done.
func (lf *ListFile) CreateIndexByUint64(indexName string, uint64ColGetter func(item []byte) uint64) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	// TODO:
	// - check if index exists
	// - if not, create and iterate over all items
	_, ok := lf.secondaryIndexes[indexName]
	if !ok {
		// stub index:
		lf.secondaryIndexes[indexName] = &Index{
			indexer: hashsearch.NewUint64(),
		}
		// create func that will add items to index during appends:
		backfillFunc := newBackfillFuncUint64(lf.secondaryIndexes[indexName], uint64ColGetter)
		lf.secondaryIndexes[indexName].backfillFunc = backfillFunc

		// do the backfill for all current items in the list:
		lf.noMutexIterateLines(backfillFunc)
	}

	return nil
}

func (lf *ListFile) IsClosed() bool {
	lf.mu.RLock()
	isClosed := lf.isClosed
	lf.mu.RUnlock()
	// if the file is already closed, any operation on it
	// will panic.
	return isClosed
}

// NumBytes returns the size in bytes of the file;
// WARNING: this operation does not lock the mutex.
func (lf *ListFile) NumBytes() int64 {
	// NOTE: here we don't use IsClosed() because
	// it uses the mutex; Size() is used in noMutexIterateLines
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

	return info.Size()
}

func (lf *ListFile) NumLines() int64 {
	if lf.IsClosed() {
		return 0
	}

	var count int64
	lf.IterateLinesAsBytes(func(_ []byte) bool {
		// TODO: does setting line to nil somehow break things?
		// Does it help with garbage collection?

		count++
		return true
	})
	return count
}

// IterateLines iterates on the lines of the list;
// this operation is LOCKING.
func (lf *ListFile) IterateLines(iterator func(line string) bool) error {
	return lf.iterateLines(func(b []byte) bool {
		return iterator(string(b))
	})
}

func (lf *ListFile) IterateLinesAsBytes(iterator func(line []byte) bool) error {
	return lf.iterateLines(iterator)
}

func (lf *ListFile) iterateLines(iterator func([]byte) bool) error {
	if lf.IsClosed() {
		return errors.New("file is already closed")
	}

	// TODO: use a Lock() or a RLock() ???
	lf.mu.RLock()
	defer lf.mu.RUnlock()

	return lf.noMutexIterateLines(iterator)
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

	// destroy all indexes:
	for i := range lf.secondaryIndexes {
		lf.secondaryIndexes[i] = nil
	}
	lf.secondaryIndexes = nil
	// delete main index (if any):
	lf.mainIndex = nil

	// mark as closed:
	lf.isClosed = true

	return nil
}
