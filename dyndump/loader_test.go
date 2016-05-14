// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"errors"
	"io"
	"reflect"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
)

// Test loading a series of items succeeds and Run exits cleanly
func TestLoadOK(t *testing.T) {
	items := newLoadItems(makeIntItem("v", 1), makeIntItem("v", 2), makeIntItem("v", 3))
	var values stringVals
	dyn := &fakeDynPuter{
		put: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			values.Add(aws.StringValue(input.Item["v"].N))
			return &dynamodb.PutItemOutput{
				ConsumedCapacity: &dynamodb.ConsumedCapacity{CapacityUnits: aws.Float64(1)},
			}, nil
		},
	}
	ld := &Loader{
		Dyn:         dyn,
		TableName:   "test-table",
		MaxParallel: 2,
		Source:      items,
	}

	done := make(chan error)
	go func() { done <- ld.Run() }()

	select {
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for Run to complete")
	case err := <-done:
		if err != nil {
			t.Error("Unexpected error from Run", err)
		}
	}

	expected := []string{"1", "2", "3"}
	if vals := values.Sorted(); !reflect.DeepEqual(vals, expected) {
		t.Error("Incorrect values sent to Dynamo", vals)
	}
}

// Test that a failure from readitem causes Run to exit with error
func TestLoadReadErr(t *testing.T) {
	testErr := errors.New("test error")
	items := newLoadItems(makeIntItem("v", 1), makeIntItem("v", 2), makeIntItem("v", 3))
	items.appendError(testErr)

	dyn := &fakeDynPuter{
		put: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return &dynamodb.PutItemOutput{
				ConsumedCapacity: &dynamodb.ConsumedCapacity{CapacityUnits: aws.Float64(1)},
			}, nil
		},
	}

	ld := &Loader{
		Dyn:         dyn,
		TableName:   "test-table",
		MaxParallel: 2,
		Source:      items,
	}

	done := make(chan error)
	go func() { done <- ld.Run() }()

	select {
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for Run to complete")
	case err := <-done:
		if err != testErr {
			t.Error("Incorrect error from Run", err)
		}
	}
}

// Test that a failure from put causes Run to exit with error
func TestLoadPutErr(t *testing.T) {
	testErr := errors.New("test error")
	items := newLoadItems(makeIntItem("v", 1), makeIntItem("v", 2), makeIntItem("v", 3))

	dyn := &fakeDynPuter{
		put: func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
			return nil, testErr
		},
	}

	ld := &Loader{
		Dyn:         dyn,
		TableName:   "test-table",
		MaxParallel: 2,
		Source:      items,
	}

	done := make(chan error)
	go func() { done <- ld.Run() }()

	select {
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for Run to complete")
	case err := <-done:
		if err != testErr {
			t.Error("Incorrect error from Run", err)
		}
	}
}

type loadItem struct {
	item map[string]*dynamodb.AttributeValue
	err  error
}

func newLoadItems(items ...map[string]*dynamodb.AttributeValue) *loadItems {
	r := new(loadItems)
	for _, i := range items {
		r.items = append(r.items, loadItem{item: i})
	}
	return r
}

type loadItems struct {
	items []loadItem
}

func (i *loadItems) appendError(err error) {
	i.items = append(i.items, loadItem{err: err})
}

func (i *loadItems) ReadItem() (item map[string]*dynamodb.AttributeValue, err error) {
	var entry loadItem

	if len(i.items) == 0 {
		return nil, io.EOF
	}

	entry, i.items = i.items[0], i.items[1:]
	return entry.item, entry.err
}

type fakeDynPuter struct {
	put func(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error)
}

func (d *fakeDynPuter) PutItem(input *dynamodb.PutItemInput) (*dynamodb.PutItemOutput, error) {
	return d.put(input)
}

type stringVals struct {
	m      sync.Mutex
	values []string
}

func (s *stringVals) Add(v string) {
	s.m.Lock()
	defer s.m.Unlock()
	s.values = append(s.values, v)
}

func (s *stringVals) Sorted() []string {
	sort.Strings(s.values)
	return s.values
}
