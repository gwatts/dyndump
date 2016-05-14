// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"fmt"
	"math"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/juju/ratelimit"
)

var (
	limitCalcSize = 50 // number of item sizes to collect when calculating an average
	initialLimit  = 20 // Iniital number of items to request when size is unknown
)

// ItemWriter is the interface expected by a Fetcher when writing retrieved
// DynamoDB items.  Must support writes from concurrent goroutines.
type ItemWriter interface {
	WriteItem(item map[string]*dynamodb.AttributeValue) error
}

// DynScanner defines the portion of the dynamodb service
// that Fetcher requires.
type DynScanner interface {
	Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

// FetcherStats is returned by Fetcher.Stats to return current global throughput statistics.
type FetcherStats struct {
	ItemsRead    int64
	BytesRead    int64
	CapacityUsed float64
}

// Fetcher fetches data from DynamoDB at a specified capacity and writes
// fetched items to a writer implementing the ItemWriter interface.
type Fetcher struct {
	Dyn            DynScanner
	TableName      string
	ConsistentRead bool       // Setting to true will use double the read capacity.
	MaxParallel    int        // Maximum number of parallel requests to make to Dynamo.
	MaxItems       int64      // Maximum (approximately) number of items to read from Dynamo.
	ReadCapacity   float64    // Average global read capacity to use for the scan.
	Writer         ItemWriter // Retrieved items are sent to this ItemWriter.

	rateLimit    *ratelimit.Bucket
	itemsRead    int64
	bytesRead    int64
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
func (f *Fetcher) Stats() FetcherStats {
	return FetcherStats{
		ItemsRead:    atomic.LoadInt64(&f.itemsRead),
		BytesRead:    atomic.LoadInt64(&f.bytesRead),
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
	limit := aws.Int64(int64(initialLimit)) // slow start
	if f.rateLimit == nil {
		limit = aws.Int64(0) // unlimited
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

		var respSize int64
		for _, item := range resp.Items {
			if err := f.Writer.WriteItem(item); err != nil {
				doneChan <- fmt.Errorf("write failed: %s", err)
				return
			}
			itemSize := calcItemSize(item)
			respSize += int64(itemSize)
			f.limitCalc.addSize(itemSize)
		}

		atomic.AddInt64(&f.itemsRead, int64(len(resp.Items)))
		atomic.AddInt64(&f.bytesRead, respSize)
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
