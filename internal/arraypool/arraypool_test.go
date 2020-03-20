package arraypool

import "testing"

func Test_Reusability(t *testing.T) {

	n := 2
	s := 128

	ap := NewArrayPool(s)
	verbose = true

	b := ap.Rent(n * s)

	if len(b) != n*s {
		t.Error("the size of returned byte array isn't valid")
	}

	ap.Release(b)

	b = ap.Rent(n * s)

	if numberOfCreatedSegments != n {
		t.Error("the number of created segments doesn't math the provided value", numberOfCreatedSegments)
	}

	if numberOfReusedSegments != n {
		t.Error("the number of reused segments doesn't match the provided value", numberOfReusedSegments)
	}

}
