// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"reflect"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

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

func TestProcessSegment(t *testing.T) {
	// Read 3 sets of data from scan, ensure it's sent correctly to a writer
	// and that processSegment exits cleanly after the last block is returned
	nextKey := -1
	retcount := 3 // number of scan results to return
	var sent []map[string]*dynamodb.AttributeValue

	dyn := &fakeDynamo{
		scan: func(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			if name := aws.StringValue(input.TableName); name != "table-name" {
				t.Error("Incorrect table name", name)
			}
			if !aws.BoolValue(input.ConsistentRead) {
				t.Error("ConsistentRead was false")
			}
			if segnum := aws.Int64Value(input.Segment); segnum != 2 {
				t.Error("Incorrect segment number", segnum)
			}
			if totalseg := aws.Int64Value(input.TotalSegments); totalseg != 4 {
				t.Error("Incorrect total segments ", totalseg)
			}
			key := intItemValue("key", input.ExclusiveStartKey)
			if key != nextKey {
				t.Errorf("Incorrect start key expected=%d actual=%d (%v)", nextKey, key, input.ExclusiveStartKey)
			}
			nextKey = key + 1
			var lastEvalKey map[string]*dynamodb.AttributeValue
			if nextKey < retcount {
				lastEvalKey = makeIntItem("key", nextKey)
			}

			items := makeItems(key*10, 3)
			sent = append(sent, items...)
			return &dynamodb.ScanOutput{
				LastEvaluatedKey: lastEvalKey,
				Items:            items,
				ConsumedCapacity: &dynamodb.ConsumedCapacity{CapacityUnits: aws.Float64(1)},
			}, nil
		},
	}

	iw := new(testItemWriter)
	f := &Fetcher{
		Dyn:            dyn,
		ConsistentRead: true,
		limitCalc:      newLimitCalc(limitCalcSize),
		TableName:      "table-name",
		MaxParallel:    4,
		ReadCapacity:   10,
		Writer:         iw,
	}

	done := make(chan error)
	go f.processSegment(2, done)

	select {
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for fetcher to complete")
	case err := <-done:
		if err != nil {
			t.Fatal("Unepxected error returned by processor", err)
		}
	}

	if !reflect.DeepEqual(iw.items, sent) {
		t.Error("Did not receive the same items as those sent")
	}
}

func TestRunOK(t *testing.T) {
	// Start four parallel readers and make sure all data was read correctly
	dyn := &fakeDynamo{
		scan: func(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
			segnum := int(aws.Int64Value(input.Segment))
			items := makeItems(segnum*10, 3)
			return &dynamodb.ScanOutput{
				Items:            items,
				ConsumedCapacity: &dynamodb.ConsumedCapacity{CapacityUnits: aws.Float64(1)},
			}, nil
		},
	}

	iw := new(testItemWriter)
	f := &Fetcher{
		Dyn:          dyn,
		limitCalc:    newLimitCalc(limitCalcSize),
		TableName:    "table-name",
		MaxParallel:  4,
		ReadCapacity: 10,
		Writer:       iw,
	}

	done := make(chan error)
	go func() { done <- f.Run() }()

	select {
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for Run to complete")
	case err := <-done:
		if err != nil {
			t.Error("Unexpected error from Run", err)
		}
	}

	expected := []int{0, 1, 2, 10, 11, 12, 20, 21, 22, 30, 31, 32}
	var actual []int
	for _, item := range iw.items {
		actual = append(actual, intItemValue("key", item))
	}
	sort.Ints(actual)

	if !reflect.DeepEqual(expected, actual) {
		t.Errorf("expected=%#v actual=%#v", expected, actual)
	}
}

// TODO: add unit tests for the rest of the thing.

// Test stop on maxitems
// Test wait on capacity used
// Test fail on scan error
// Test fail on write error

func makeIntItem(key string, value int) map[string]*dynamodb.AttributeValue {
	return map[string]*dynamodb.AttributeValue{
		key: &dynamodb.AttributeValue{
			N: aws.String(strconv.FormatInt(int64(value), 10)),
		},
	}
}

func intItemValue(key string, item map[string]*dynamodb.AttributeValue) int {
	if entry, ok := item[key]; ok {
		v, _ := strconv.Atoi(aws.StringValue(entry.N))
		return v
	}
	return -1
}

func makeItems(offset, count int) (result []map[string]*dynamodb.AttributeValue) {
	for i := offset; i < offset+count; i++ {
		result = append(result, makeIntItem("key", i))
	}
	return result
}

type fakeDynamo struct {
	scan func(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error)
}

func (fd *fakeDynamo) Scan(input *dynamodb.ScanInput) (*dynamodb.ScanOutput, error) {
	return fd.scan(input)
}

// testItemWriter just collects all written items
type testItemWriter struct {
	m     sync.Mutex
	items []map[string]*dynamodb.AttributeValue
}

func (iw *testItemWriter) WriteItem(item map[string]*dynamodb.AttributeValue) error {
	iw.m.Lock()
	iw.items = append(iw.items, item)
	iw.m.Unlock()
	return nil
}
