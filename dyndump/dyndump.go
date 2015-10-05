// Copyright 2015 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

/*
Package dyndump provides a scan-based method for dumping an entire DynamoDB
table using scan.

It supports parallel connections to the server for increased throughput along
with rate limiting to a specific read capacity.

Items are written to an ItemWriter interface until the table is exhausted,
or the Stop method is called.
*/
package dyndump

import (
	"fmt"
	"math"
	"time"

	"sort"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/juju/ratelimit"
)

var (
	limitCalcSize = 50 // number of item sizes to collect when calculating an average
)

// Stats is returned by Fetcher.Stats to return current global throughput statistics.
type Stats struct {
	ItemsRead    int64
	CapacityUsed float64
}

// Fetcher fetches data from DynamoDB at a specified capacity and writes
// fetched items to a writer implementing the ItemWriter interface.
type Fetcher struct {
	Dyn            *dynamodb.DynamoDB
	TableName      string
	ConsistentRead bool       // Setting to true will use double the read capacity.
	MaxParallel    int        // Maximum number of parallel requests to make to Dynamo.
	MaxItems       int64      // Maximum (approximately) number of items to read from Dynamo.
	ReadCapacity   float64    // Average global read capacity to use for the scan.
	Writer         ItemWriter // Retrieved items are sent to this ItemWriter.

	rateLimit    *ratelimit.Bucket
	itemsRead    int64
	capacityUsed int64 // multiplied by 10
	stopRequest  chan struct{}
	stopNotify   chan struct{}
	limitCalc    *limitCalc
}

// Run executes the fetcher, starting as many parallel reads as specified by
// the MaxParallel option and returns when the read has finished, failed, or
// been stopped.
func (f *Fetcher) Run() error {
	errChan := make(chan error, f.MaxParallel)
	f.stopRequest = make(chan struct{}, 2)
	f.stopNotify = make(chan struct{})
	f.limitCalc = newLimitCalc(limitCalcSize)

	if f.ReadCapacity > 0 {
		f.rateLimit = ratelimit.NewBucketWithQuantum(time.Second, int64(f.ReadCapacity), int64(f.ReadCapacity))
	}

	go func() {
		<-f.stopRequest
		close(f.stopNotify) // fanout
	}()

	for i := int64(0); i < int64(f.MaxParallel); i++ {
		go f.processSegment(i, errChan)
	}

	var err error
	// wait for all workers to shutdown
	for i := 0; i < f.MaxParallel; i++ {
		if werr := <-errChan; werr != nil {
			if err == nil {
				err = werr
				f.stopRequest <- struct{}{}
			}
		}
	}
	return err
}

// Stop requests a clean shutdown of active readers.
// Active readers will complete the current request and then exit.
func (f *Fetcher) Stop() {
	f.stopRequest <- struct{}{}
}

// Stats returns current aggregate statistics about an ongoing or completed run.
// It is safe to call from concurrent goroutines.
func (f *Fetcher) Stats() Stats {
	return Stats{
		ItemsRead:    atomic.LoadInt64(&f.itemsRead),
		CapacityUsed: float64(atomic.LoadInt64(&f.capacityUsed)) / 10,
	}
}

func (f *Fetcher) isStopped() bool {
	select {
	case <-f.stopNotify:
		return true
	default:
		return false
	}
}

// Interruptible rate limit wait
// Returns true if Stop() was called while waiting.
func (f *Fetcher) waitForRateLimit(usedCapacity int64) bool {
	d := f.rateLimit.Take(usedCapacity)
	if d > 0 {
		select {
		case <-time.After(d):
			return false
		case <-f.stopNotify:
			return true
		}
	}
	return false
}

// process a single segment.  executed in a separate goroutine by Run
// for parallel scans.
func (f *Fetcher) processSegment(segNum int64, doneChan chan<- error) {
	limit := aws.Int64(20) // slow start
	if f.rateLimit == nil {
		limit = aws.Int64(0) // unlimit
	}

	params := &dynamodb.ScanInput{
		TableName:              aws.String(f.TableName),
		ConsistentRead:         aws.Bool(f.ConsistentRead),
		Limit:                  limit,
		Segment:                aws.Int64(segNum),
		TotalSegments:          aws.Int64(int64(f.MaxParallel)),
		ReturnConsumedCapacity: aws.String("TOTAL"),
	}

	usedCapacity := int64(1)
	for {
		if f.rateLimit != nil {
			if isStopped := f.waitForRateLimit(usedCapacity); isStopped {
				break
			}
		}

		if f.isStopped() {
			break
		}

		// the dynamo service will automatically retry soft errors (including hitting capacity limits)
		// with a backoff algorithm any other errors returned are hard errors
		resp, err := f.Dyn.Scan(params)
		if err != nil {
			doneChan <- fmt.Errorf("read from DynamoDB failed: %s", err)
			return
		}

		respSize := 0
		for _, item := range resp.Items {
			if err := f.Writer.WriteItem(item); err != nil {
				doneChan <- fmt.Errorf("write failed: %s", err)
				return
			}
			itemSize := calcItemSize(item)
			respSize += itemSize
			f.limitCalc.addSize(itemSize)
		}

		atomic.AddInt64(&f.itemsRead, int64(len(resp.Items)))
		atomic.AddInt64(&f.capacityUsed, int64(*resp.ConsumedCapacity.CapacityUnits*10))
		if f.MaxItems > 0 && atomic.LoadInt64(&f.itemsRead) >= f.MaxItems {
			break
		}

		if resp.LastEvaluatedKey == nil {
			// all data scanned
			break
		}

		usedCapacity = int64(math.Ceil(*resp.ConsumedCapacity.CapacityUnits))
		params.ExclusiveStartKey = resp.LastEvaluatedKey
		if f.rateLimit != nil {
			if newLimit := f.calcLimit(); newLimit > 0 {
				params.Limit = aws.Int64(int64(newLimit))
			}
		}
	}
	doneChan <- nil
}

// adjust the fetch limit amount to approximate the desired read capacity and
// make effective use of 4k blocks for small items
func (f *Fetcher) calcLimit() (newLimit int) {
	desiredCapacity := f.ReadCapacity / float64(f.MaxParallel)

	// find the median item size based on recent history
	medianSize := f.limitCalc.median()
	if medianSize <= 0 {
		return -1 // not enough data
	}

	itemsPer4k := float64(4096) / float64(medianSize)
	newLimit = int(itemsPer4k * desiredCapacity)
	if !f.ConsistentRead {
		newLimit *= 2
	}

	if newLimit < 1 {
		newLimit = 1
	}

	return newLimit
}

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
