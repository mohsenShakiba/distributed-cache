package arraypool

type item struct {
	offset int64
	length int64
	contentLength int64
	deleted bool
}

type ArrayPool struct {
	size int64
	segmentSize int64
	items []item
	buffer []byte
}

func NewArrayPool(size int64, segmentSize int64) *ArrayPool {
	return &ArrayPool{
		size:        size,
		segmentSize: segmentSize,
		items:       make([]item, 1000),
		buffer:      make([]byte, size),
	}
}

func (ap *ArrayPool) Rent(size int64) int64 {

	// the index of visited items
	var indexVisited int64 = 0
	for _, item := range ap.items {
		indexVisited += item.length

		// if deleted
		if !item.deleted {
			continue
		}

		// if size doesn't match
		if item.length < size {
			continue
		}

		return item.offset
	}

	// not item was found we must create a new item
	ap.items = append(ap.items, item{
		offset:        indexVisited,
		length:        getSizeInSegment(size, ap.segmentSize),
		contentLength: size,
		deleted:       false,
	})

	// check if byte array size is enough
	if ap.size >= indexVisited + getSizeInSegment(size, ap.segmentSize) {
		return indexVisited
	}

	// if not expand the array pool
	newByteArr := make([]byte, ap.size * 2)
	ap.size = ap.size * 2
	ap.buffer = newByteArr

	return indexVisited
}

func getSizeInSegment(size int64, segmentSize int64) int64 {
	remaining := size % segmentSize
	return remaining + 1
}

func RetrieveContent(index int64) []byte {

}

func Release(index int64) {

}
