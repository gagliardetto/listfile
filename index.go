package listfile

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
