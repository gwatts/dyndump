// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"crypto/sha256"
	"fmt"
	"hash"
	"sort"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gwatts/gomisc/reorder"
	"github.com/juju/ratelimit"
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

type rateLimitWaiter struct {
	*ratelimit.Bucket
	stopNotify chan struct{}
}

// Interruptible rate limit wait
// Returns true if the stopChan was closed while waiting
func (w *rateLimitWaiter) waitForRateLimit(usedCapacity int64) bool {
	d := w.Take(usedCapacity)
	if d > 0 {
		select {
		case <-time.After(d):
			return false
		case <-w.stopNotify:
			return true
		}
	}
	return false
}

// hashCalc receives sha256 hashes of individual parts in whatever
// order they're written and constructs a entire-backup hash which
// consists of a hash of newline-terminated part hashes in order
// sha256("parthash1\nparthash2\n")
type hashCalc struct {
	m      sync.Mutex
	buf    *reorder.Buffer
	h      hash.Hash
	hipart int
}

func newHashCalc() *hashCalc {
	hc := &hashCalc{
		h:      sha256.New(),
		hipart: -1,
	}
	hc.buf = reorder.NewBuffer(0, reorder.WriterFunc(func(n int, buf []interface{}) error {
		// hc.m is already held at the point this is called
		for _, entry := range buf {
			h := entry.([]byte)
			fmt.Fprintf(hc.h, "%x\n", h)
		}
		hc.hipart = n + len(buf)
		return nil
	}))

	return hc
}

func (hc *hashCalc) add(pn int, h hash.Hash) error {
	hc.m.Lock()
	defer hc.m.Unlock()
	return hc.buf.Add(pn, h.Sum(nil))
}

func (hc *hashCalc) value() (int, string) {
	hc.m.Lock()
	defer hc.m.Unlock()
	return hc.hipart, fmt.Sprintf("%x", hc.h.Sum(nil))
}
