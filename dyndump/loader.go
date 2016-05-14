// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"io"
	"math"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/juju/ratelimit"
)

// ItemReader is the interface expected by a Loader to retrieve items from
// a source for loading into a DynamoDB table.
type ItemReader interface {
	ReadItem() (item map[string]*dynamodb.AttributeValue, err error)
}

// DynPuter defines the portion of the DynamoDB service the Loader requires.
type DynPuter interface {
	PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

// LoaderStats are returned by Loader.Stats
type LoaderStats struct {
	ItemsWritten int64
	ItemsSkipped int64
	BytesWritten int64
	CapacityUsed float64
}

// Loader reads records from an ItemReader and loads them into a DynamoDB
// table.
type Loader struct {
	Dyn            DynPuter
	TableName      string     // Table name to restore to
	MaxParallel    int        // Maximum number of put operations to execute concurrently
	MaxItems       int64      // Maximum (approximately) number of items to write to Dynamo.
	WriteCapacity  float64    // Maximum Dynamo write capacity to use for writes
	Source         ItemReader // The source to fetch items from
	AllowOverwrite bool       // If true then any existing records will be ovewritten
	HashKey        string     // The attribute name of the hash key for the table

	rateLimit    *rateLimitWaiter
	itemsWritten int64
	itemsSkipped int64
	bytesWritten int64
	capacityUsed int64 // multiplied by 10
	stopRequest  chan struct{}
	stopNotify   chan struct{}
}

// Run executes the loader, starting goroutines to execute parallel puts
// as required.  Returns when the load has finished, failed or been stopped.
func (ld *Loader) Run() error {
	errChan := make(chan error, ld.MaxParallel)
	itemsChan := make(chan map[string]*dynamodb.AttributeValue)
	readDone := make(chan error)

	ld.stopRequest = make(chan struct{}, 2)
	ld.stopNotify = make(chan struct{})

	if ld.WriteCapacity > 0 {
		ld.rateLimit = &rateLimitWaiter{
			Bucket:     ratelimit.NewBucketWithQuantum(time.Second, int64(ld.WriteCapacity), int64(ld.WriteCapacity)),
			stopNotify: ld.stopNotify,
		}
	}

	go func() {
		<-ld.stopRequest
		close(ld.stopNotify) // fanout
	}()

	go func() {
		var rc int64
		for {
			select {
			case <-ld.stopNotify:
				readDone <- nil
				return

			default:
				item, err := ld.Source.ReadItem()
				if err == io.EOF {
					readDone <- nil
					return
				} else if err != nil {
					readDone <- err
					return
				}
				itemsChan <- item
				rc++
				if rc == ld.MaxItems {
					readDone <- nil
					return
				}
			}
		}
	}()

	for i := int64(0); i < int64(ld.MaxParallel); i++ {
		go ld.load(itemsChan, errChan)
	}

	// wait for either the reader or a writer to finish or fail
	rem := ld.MaxParallel
	var err error
	select {
	case err = <-readDone:
		// reader exited
		ld.Stop()

	case err = <-errChan:
		rem--
		ld.Stop()
	}

	// wait for all workers to shutdown
	for i := 0; i < rem; i++ {
		if werr := <-errChan; werr != nil {
			if err == nil {
				err = werr
				ld.stopRequest <- struct{}{}
			}
		}
	}
	return err
}

// Stop requests a clean shutdown of current put operations.  It does not
// block.  It will cause Run to exit when the loaders finish.
func (ld *Loader) Stop() {
	ld.stopRequest <- struct{}{}
}

// Stats return the current loader statistics.
func (ld *Loader) Stats() LoaderStats {
	return LoaderStats{
		ItemsWritten: atomic.LoadInt64(&ld.itemsWritten),
		ItemsSkipped: atomic.LoadInt64(&ld.itemsSkipped),
		BytesWritten: atomic.LoadInt64(&ld.bytesWritten),
		CapacityUsed: float64(atomic.LoadInt64(&ld.capacityUsed)) / 10,
	}
}

func (ld *Loader) load(items chan map[string]*dynamodb.AttributeValue, doneChan chan<- error) {
	usedCapacity := int64(1)

	for {
		select {
		case <-ld.stopNotify:
			doneChan <- nil
			return

		case item := <-items:
			if ld.rateLimit != nil {
				ld.rateLimit.waitForRateLimit(usedCapacity)
			}
			req := &dynamodb.PutItemInput{
				TableName: aws.String(ld.TableName),
				Item:      item,
				ReturnConsumedCapacity: aws.String("TOTAL"),
			}
			if !ld.AllowOverwrite {
				req.ConditionExpression = aws.String("attribute_not_exists(#K)")
				req.ExpressionAttributeNames = map[string]*string{
					"#K": aws.String(ld.HashKey),
				}
			}

			resp, err := ld.Dyn.PutItem(req)
			if err != nil {
				if aerr, ok := err.(awserr.Error); ok {
					if aerr.Code() == "ConditionalCheckFailedException" {
						atomic.AddInt64(&ld.itemsSkipped, 1)
						// without a response available, we can't know for sure the
						// capacity that was consumed; make a rough calculation
						itemSize := float64(calcItemSize(item))
						usedCapacity = int64(math.Ceil(itemSize / 1000))
						continue
					}
				}
				doneChan <- err
				return
			}

			usedCapacity = int64(math.Ceil(*resp.ConsumedCapacity.CapacityUnits))
			atomic.AddInt64(&ld.itemsWritten, 1)
			atomic.AddInt64(&ld.bytesWritten, int64(calcItemSize(item)))
			atomic.AddInt64(&ld.capacityUsed, int64(*resp.ConsumedCapacity.CapacityUnits*10))
		}
	}
}
