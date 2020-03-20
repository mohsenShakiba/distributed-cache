package arraypool

// for testing purposes only
var verbose = false
var numberOfReusedSegments = 0
var numberOfCreatedSegments = 0

// ArrayPool will provide reusable array buffer to reduce GC pressure
type ArrayPool struct {
	available chan []byte
	baseSize  int
}

// creates a new array pool
func NewArrayPool(baseSize int) *ArrayPool {

	return &ArrayPool{
		available: make(chan []byte, 10_000),
		baseSize:  baseSize,
	}

}

// Rent will provide a byte array that is either reused or created
func (ap *ArrayPool) Rent(size int) []byte {

	// check if any available byte array exists
	var bytes []byte
	for {
		select {
		case b := <-ap.available:

			bytes = append(bytes, b...)

			if len(bytes) >= size {

				if verbose {
					onByteArrayReused(len(bytes) / ap.baseSize)
				}

				return bytes
			}

		default:

			// put back the bytes retrieved pa
			if len(bytes) > 0 {
				ap.Release(bytes)
			}

			if verbose {
				onByteArrayCreated(size / ap.baseSize)
			}

			return make([]byte, size)
		}
	}

}

// Release will put back the byte array for later reuse
func (ap *ArrayPool) Release(ba []byte) {
	select {
	case ap.available <- ba:
		break
	default:
		break
	}
}

func onByteArrayReused(count int) {
	numberOfReusedSegments += count
}

func onByteArrayCreated(count int) {
	numberOfCreatedSegments += count
}
