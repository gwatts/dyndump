// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3NewKey(t *testing.T) {
	w := &S3Writer{
		PathPrefix: "aprefix",
	}

	k := w.newKey()
	if expected := "aprefix-part-000000001.json.gz"; k != expected {
		t.Errorf("expected=%q actual=%q", expected, k)
	}

	k = w.newKey()
	if expected := "aprefix-part-000000002.json.gz"; k != expected {
		t.Errorf("expected=%q actual=%q", expected, k)
	}
}

// Setup a writer, send data to it check the the data is sent to s3
// and shuts down cleanly.
// As this sets MaxParallel > 1 it should test for races too when the race
// detector is on.
func TestS3OK(t *testing.T) {
	const chunkSize = MinPartSize
	fs3 := newFakeS3()
	var md Metadata
	w := NewS3Writer(fs3, "test-bucket", "test-prefix", md)
	w.PartSize = chunkSize * 16 // Ensure multiple writes per part, multiple parts overall

	done := make(chan error)
	go func() {
		done <- w.Run()
	}()

	for i := 0; i < 256; i++ {
		// Write a block of random bytes, prefixed with the seed number
		// used to generate the pseudo random data
		rnd := append([]byte{byte(i)}, randbytes(i, chunkSize)...)
		if _, err := w.Write(rnd); err != nil {
			t.Fatalf("Write %d failed: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal("Close failed", err)
	}

	// Wait for completion
	select {
	case err := <-done:
		if err != nil {
			t.Fatal("Unexpected error from Run()", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for Run() to complete")
	}

	// check that all the data was written correctly
	seen := make(map[byte]string) // map seed number to s3 key
	for s3key, v := range fs3.parts {
		if v.bucket != "test-bucket" {
			t.Error("Incorrect bucket", v.bucket)
		}
		if v.enc != "gzip" {
			t.Error("Incorrect encoding", v.enc)
		}
		if v.ctype != "application/json" {
			t.Error("Incorrect content type", v.ctype)
		}
		if !strings.HasPrefix(s3key, "test-prefix") {
			t.Error("Incorrect key name", s3key)
		}
		// Data was written in blocks of 51 bytes; make sure we got whole multiples of those blocks
		if len(v.data)%(chunkSize+1) != 0 {
			t.Errorf("Incorrect data length %d (%d)", len(v.data), len(v.data)%(chunkSize+1))
		}
		for i := 0; i < len(v.data); i += chunkSize + 1 {
			seed := v.data[i]
			data := v.data[i+1 : i+chunkSize+1]
			if prevkey, ok := seen[seed]; ok {
				t.Fatalf("Duplicate block %d first seen in s3 key %q, seen again in %q", seed, prevkey, s3key)
			}
			if !reflect.DeepEqual(data, randbytes(int(seed), chunkSize)) {
				fmt.Printf("seed=%d expected/actual=\n%0x\n\n%0x\n", seed, randbytes(int(seed), chunkSize), data)
				t.Errorf("Incorrect data for s3key=%q seed=%d", s3key, seed)
			}
			seen[seed] = s3key
		}
	}

	// Check no seeds were missed
	if len(seen) != 256 {
		t.Error("Incorrect number of seeds seen", len(seen))
	}
}

// Test that a hard put failure results in the writer shutting down
func TestS3PutFail(t *testing.T) {
	var md Metadata
	const chunkSize = 500
	var failError = errors.New("Failed")
	s3 := fakePutObject(func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
		if k := aws.StringValue(input.Key); strings.Contains(k, "meta.json") {
			return nil, nil // let puts for metadata succeed
		}
		return nil, failError
	})

	w := NewS3Writer(s3, "test-bucket", "test-prefix", md)
	w.PartSize = MinPartSize

	done := make(chan error)
	go func() {
		done <- w.Run()
	}()

	// Run writes until we get a fail
	var err error
	errch := make(chan error)
	go func() {
		for i := 0; i < 100; i++ {
			if _, err = w.Write(randbytes(i, chunkSize)); err != nil {
				errch <- err
				return
			}
		}
	}()

	select {
	case err = <-errch:
	case err := <-done:
		t.Fatal("Run exited unexpectedly", err)
	}

	if err == nil {
		t.Fatal("No error received from write")
	}

	if err != failError {
		t.Error("Incorrect error returned from Write", err)
	}

	if err := w.Close(); err != failError {
		t.Error("Close returned incorrect error", err)
	}

	// Wait for completion
	select {
	case err := <-done:
		if err != failError {
			t.Fatal("Incorrect error from Run", err)
		}
	case <-time.After(time.Second):
		t.Fatal("Timeout waiting for Run to complete")
	}
}

type putdata struct {
	data   []byte
	bucket string
	enc    string
	ctype  string
}

type fakeS3 struct {
	m        sync.Mutex
	metadata []byte
	parts    map[string]putdata
}

func newFakeS3() *fakeS3 {
	return &fakeS3{parts: make(map[string]putdata)}
}

func (fs3 *fakeS3) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	k := aws.StringValue(input.Key)
	bucket := aws.StringValue(input.Bucket)

	if strings.HasSuffix(k, "meta.json") {
		data, err := ioutil.ReadAll(input.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to read body for key %s: %v", k, err)
		}
		fs3.m.Lock()
		fs3.metadata = data
		fs3.m.Unlock()
	} else {
		// gunzip the data and store that
		gzr, err := gzip.NewReader(input.Body)
		if err != nil {
			return nil, fmt.Errorf("Failed to gunzip key %s: %v", k, err)
		}

		data, err := ioutil.ReadAll(gzr)
		if err != nil {
			return nil, fmt.Errorf("Failed to read body for key %s: %v", k, err)
		}

		fs3.m.Lock()
		fs3.parts[k] = putdata{
			data:   data,
			bucket: bucket,
			enc:    aws.StringValue(input.ContentEncoding),
			ctype:  aws.StringValue(input.ContentType),
		}
		fs3.m.Unlock()
	}
	return nil, nil
}

type fakePutObject func(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)

func (f fakePutObject) PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
	return f(input)
}

func randbytes(seed, qty int) (result []byte) {
	rnd := rand.New(rand.NewSource(int64(seed)))
	result = make([]byte, qty)
	for i := 0; i < qty; i++ {
		result[i] = byte(rnd.Intn(255))
	}
	return result
}
