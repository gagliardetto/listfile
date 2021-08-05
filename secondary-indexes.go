package listfile

import (
	"github.com/gagliardetto/hashsearch"
)

type Index struct {
	indexer      indexerWrapper
	backfillFunc func(line []byte) bool
}

type indexerWrapper interface {
	Has(interface{}) bool
	Add(interface{})
}

var _ indexerWrapper = &indexerWrapperUint64{}
var _ indexerWrapper = &indexerWrapperInt{}

type indexerWrapperUint64 struct {
	indexer *hashsearch.Uint64
}

func newIndexerWrapperUint64() *indexerWrapperUint64 {
	return &indexerWrapperUint64{
		indexer: hashsearch.NewUint64(),
	}
}
func (wrap *indexerWrapperUint64) Has(val interface{}) bool {
	return wrap.indexer.Has(val.(uint64))
}
func (wrap *indexerWrapperUint64) Add(val interface{}) {
	wrap.indexer.Add(val.(uint64))
}

func newBackfillFuncUint64(index *Index, uint64ColGetter func(item []byte) uint64) func(line []byte) bool {
	return func(line []byte) bool {
		colVal := uint64ColGetter(line)
		index.indexer.Add(colVal)
		return true
	}
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
	return index.indexer.(*indexerWrapperUint64).indexer.Has(val)
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
			indexer: newIndexerWrapperUint64(),
		}
		// create func that will add items to index during appends:
		backfillFunc := newBackfillFuncUint64(lf.secondaryIndexes[indexName], uint64ColGetter)
		lf.secondaryIndexes[indexName].backfillFunc = backfillFunc

		// do the backfill for all current items in the list:
		lf.noMutexIterateLines(backfillFunc)
	}

	return nil
}

type indexerWrapperInt struct {
	indexer *hashsearch.Int
}

func newIndexerWrapperInt() *indexerWrapperInt {
	return &indexerWrapperInt{
		indexer: hashsearch.NewInt(),
	}
}
func (wrap *indexerWrapperInt) Has(val interface{}) bool {
	return wrap.indexer.Has(val.(int))
}
func (wrap *indexerWrapperInt) Add(val interface{}) {
	wrap.indexer.Add(val.(int))
}

func newBackfillFuncInt(index *Index, intColGetter func(item []byte) int) func(line []byte) bool {
	return func(line []byte) bool {
		colVal := intColGetter(line)
		index.indexer.Add(colVal)
		return true
	}
}

// HasIntByIndex checks whether the index contains a specific value.
func (lf *ListFile) HasIntByIndex(indexName string, val int) bool {
	lf.mu.RLock()
	defer lf.mu.RUnlock()
	index, ok := lf.secondaryIndexes[indexName]
	if !ok {
		// TODO: if the index does not exist, return error, or create it, or just return false?
		return false
	}
	return index.indexer.(*indexerWrapperInt).indexer.Has(val)
}

// CreateIndexByInt creates a new index using the int value returned by the intColGetter
// When the index is created, a backfill is executed on all exisitng lines in the listfile.
// If the index already exists, nothing is done.
func (lf *ListFile) CreateIndexByInt(indexName string, intColGetter func(item []byte) int) error {
	lf.mu.Lock()
	defer lf.mu.Unlock()
	// TODO:
	// - check if index exists
	// - if not, create and iterate over all items
	_, ok := lf.secondaryIndexes[indexName]
	if !ok {
		// stub index:
		lf.secondaryIndexes[indexName] = &Index{
			indexer: newIndexerWrapperInt(),
		}
		// create func that will add items to index during appends:
		backfillFunc := newBackfillFuncInt(lf.secondaryIndexes[indexName], intColGetter)
		lf.secondaryIndexes[indexName].backfillFunc = backfillFunc

		// do the backfill for all current items in the list:
		lf.noMutexIterateLines(backfillFunc)
	}

	return nil
}
