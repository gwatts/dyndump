// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	defaultSliceSize   = 10 * 1024 * 1024
	defaultMaxParallel = 2
	minSliceSize       = 1000
)

// S3 defines the portion of the s3 service that S3Writer requires
type S3 interface {
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

// S3Writer takes a stream of JSON data and uploads it
// in parallel to multiple S3 objects.
type S3Writer struct {
	S3          S3
	Bucket      string
	PathPrefix  string
	SliceSize   int // number of bytes to store each slice
	MaxParallel int // Maximum number of parallel uploads to perform to S3

	partnum int32
	data    chan []byte // workers read from this channel
	wg      sync.WaitGroup
	fm      sync.Mutex
	failed  error
}

// NewS3Writer creates and initalizes a new S3Writer
func NewS3Writer(s3 S3, bucket, pathPrefix string) *S3Writer {
	return &S3Writer{
		S3:          s3,
		Bucket:      bucket,
		PathPrefix:  pathPrefix,
		SliceSize:   defaultSliceSize,
		MaxParallel: defaultMaxParallel,
		data:        make(chan []byte),
	}
}

// Run starts goroutines to feed incoming data sent to Write to S3.
func (w *S3Writer) Run() error {
	if w.data == nil {
		w.data = make(chan []byte)
	}
	if w.SliceSize < minSliceSize {
		return errors.New("SliceSize too small")
	}
	if w.MaxParallel < 1 {
		return errors.New("MaxParallel must be 1 or greater")
	}
	for i := 0; i < w.MaxParallel; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	w.wg.Wait()
	return w.failError()
}

// Write takes a single block of JSON text and sends it off to S3.
// It will return an error if a Put to S3 has failed.
func (w *S3Writer) Write(p []byte) (n int, err error) {
	if err := w.failError(); err != nil {
		return 0, err // previously failed
	}
	w.data <- append([]byte{}, p...)
	return len(p), nil
}

// Close causes the writers to finish processing their uploads
// and will cause Run to exit once they finish.
func (w *S3Writer) Close() error {
	w.fm.Lock()
	defer w.fm.Unlock()
	close(w.data)
	return w.failed
}

// newKey generates the next S3 object key.
func (w *S3Writer) newKey() string {
	pn := atomic.AddInt32(&w.partnum, 1)
	return fmt.Sprintf("%s-%09d.json.gz", w.PathPrefix, pn)
}

// fail sets the failure error, if not already set
// active writers will discard any further data received.
func (w *S3Writer) fail(err error) {
	w.fm.Lock()
	if w.failed == nil {
		w.failed = err
	}
	w.fm.Unlock()
}

// failError returns the error sent to fail()
func (w *S3Writer) failError() error {
	w.fm.Lock()
	defer w.fm.Unlock()
	return w.failed
}

func (w *S3Writer) worker() {
	var failed bool

	defer w.wg.Done()

	// pre-allocate space
	buf := bytes.NewBuffer(make([]byte, w.SliceSize))
	buf.Reset()

	gz := gzip.NewWriter(buf)

	flush := func() error {
		if err := w.failError(); err != nil {
			failed = true
			return err
		}
		gz.Close()
		req := &s3.PutObjectInput{
			Bucket:          aws.String(w.Bucket),
			Key:             aws.String(w.newKey()),
			Body:            bytes.NewReader(buf.Bytes()),
			ContentEncoding: aws.String("gzip"),
			ContentType:     aws.String("application/json"),
		}
		_, err := w.S3.PutObject(req)
		if err != nil {
			return err
		}
		buf.Reset()
		gz.Reset(buf)
		return nil
	}

	var intervalBytes int
	gzipFlushInterval := w.SliceSize / 10
	for data := range w.data {
		if failed {
			continue
		}
		gz.Write(data)
		intervalBytes += len(data)
		if intervalBytes >= gzipFlushInterval {
			gz.Flush() // Flush to get a sense of how much data is buffered
			intervalBytes = 0
		}
		if buf.Len() >= w.SliceSize {
			if err := flush(); err != nil {
				w.fail(err)
				failed = true
			}
		}
	}

	if intervalBytes > 0 && !failed {
		if err := flush(); err != nil {
			w.fail(err)
		}
	}
}
