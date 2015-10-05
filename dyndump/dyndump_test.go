// Copyright 2015 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import "testing"

func setLimitMedian(lc *limitCalc, median int) {
	for i := 0; i < len(lc.itemSizes); i++ {
		lc.addSize(median)
	}
}

var limitTests = []struct {
	medianSize      int
	desiredCapacity float64
	parallel        int
	expectedLimit   int
}{
	{10, 1, 1, 409},   // 409 10 byte items fits into 4k
	{10000, 10, 1, 4}, // 0.4 10k items per 4k, * 10
	{10000, 1, 1, 1},  // 0.4 10k items per 4k, round up to min 1 item read
	{10000, 10, 2, 2}, // 0.4 10k items per 4k, * 10 / (2 concurrent)
}

func TestCalcLimit(t *testing.T) {
	for _, test := range limitTests {
		f := &Fetcher{ReadCapacity: test.desiredCapacity, MaxParallel: test.parallel, limitCalc: newLimitCalc(5), ConsistentRead: true}
		setLimitMedian(f.limitCalc, test.medianSize)
		newLimit := f.calcLimit()
		if newLimit != test.expectedLimit {
			t.Errorf("Input=%#v expected=%d actual=%d", test, test.expectedLimit, newLimit)
		}
	}
}

func TestCalcConsistentLimit(t *testing.T) {
	f := &Fetcher{ReadCapacity: 1, MaxParallel: 1, limitCalc: newLimitCalc(5), ConsistentRead: true}
	setLimitMedian(f.limitCalc, 10)
	if limit := f.calcLimit(); limit != 409 {
		t.Error("Incorrect limit for consistent read", limit)
	}

	f = &Fetcher{ReadCapacity: 1, MaxParallel: 1, limitCalc: newLimitCalc(5), ConsistentRead: false}
	setLimitMedian(f.limitCalc, 10)
	if limit := f.calcLimit(); limit != 409*2 {
		t.Error("Incorrect limit for inconsistent read", limit)
	}
}

// TODO: add unit tests for the rest of the thing.
