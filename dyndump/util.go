// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"sort"
	"sync"

	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// this is based on https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/WorkingWithTables.html#ItemSizeCalculations
func calcItemSize(item map[string]*dynamodb.AttributeValue) (size int) {
	for k, av := range item {
		size += len(k)
		size += calcAttrSize(av)
	}
	return size
}

func calcAttrSize(av *dynamodb.AttributeValue) (size int) {
	switch {
	case av.B != nil: // binary
		size += len(av.B)

	case av.BOOL != nil: // Bool
		size++

	case av.BS != nil: // binary set
		size += 3
		for _, v := range av.BS {
			size += len(v)
		}

	case av.L != nil: // list of attributes
		size += 3
		for _, v := range av.L {
			size += calcAttrSize(v)
		}

	case av.M != nil: // map of attributes
		size += 3
		for k, v := range av.M {
			size += len(k) + calcAttrSize(v)
		}

	case av.N != nil: // number
		size += len(*av.N)

	case av.NS != nil: // number set
		size += 3
		for _, v := range av.NS {
			size += len(*v)
		}

	case av.NULL != nil: // null
		size++

	case av.S != nil: // string
		size += len(*av.S)

	case av.SS != nil: // string set
		size += 3
		for _, v := range av.SS {
			size += len(*v)
		}
	}
	return size
}

// track recent sizes of items
type limitCalc struct {
	m         sync.Mutex
	itemSizes []int
	offset    int64
}

func newLimitCalc(size int) *limitCalc {
	return &limitCalc{itemSizes: make([]int, size)}
}

func (lc *limitCalc) addSize(size int) {
	lc.m.Lock()
	defer lc.m.Unlock()
	lc.itemSizes[lc.offset%int64(len(lc.itemSizes))] = size
	lc.offset++
}

func (lc *limitCalc) median() int {
	lc.m.Lock()
	defer lc.m.Unlock()
	if lc.offset < int64(len(lc.itemSizes)) {
		return -1
	}
	sort.Ints(lc.itemSizes)
	return lc.itemSizes[len(lc.itemSizes)/2] // close enough to median
}
