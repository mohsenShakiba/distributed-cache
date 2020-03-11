package arraypool

import (
	"errors"
)

type ArrayPool struct {
	available chan []byte
	width     int64
	maxSize   int64
}

func NewArrayPool(maxSize int64, initialSize int64, width int64) (*ArrayPool, error) {

	if maxSize < initialSize {
		return nil, errors.New("Invalid max size")
	}

	if initialSize < width {
		return nil, errors.New("Invalid initial size")
	}

	return &ArrayPool{
		available: make(chan []byte, initialSize),
		width:     width,
		maxSize:   maxSize,
	}, nil
}

func (ap *ArrayPool) Rent() []byte {

	// check if any available byte array exists

	select {
	case b := <-ap.available:
		return b
	default:
		return ap.createNewByteArray()
	}

}

func (ap *ArrayPool) createNewByteArray() []byte {
	return make([]byte, ap.width)
}

func (ap *ArrayPool) Release(ba []byte) {
	select {
	case ap.available <- ba:
		break
	default:
		break
	}
}
