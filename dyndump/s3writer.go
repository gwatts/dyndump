// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"io"
	"io/ioutil"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	// DefaultPartSize sets the default maximum size of objects sent to S3.
	DefaultPartSize = 50 * 1024 * 1024 // 50 MiB

	// DefaultS3MaxParallel sets the default maximum number of concurrent
	// write requests for S3.
	DefaultS3MaxParallel = 2

	// MinPartSize defines the minimum value that can be used for PartSize.
	MinPartSize = 1000
)

const (
	metaSha256     = "dyndump-sha256"
	metaItemCount  = "dyndump-itemcount"
	metaPartNumber = "dyndump-part"
)

// S3Puter defines the portion of the S3 service required by S3Writer.
type S3Puter interface {
	PutObject(input *s3.PutObjectInput) (*s3.PutObjectOutput, error)
}

// S3Writer takes a stream of JSON data and uploads it
// in parallel to S3.
//
// It divides the stream into multiple pieces which store a maximum of
// approximately PartSize bytes each.
//
// Each part is given a key name beginning with PathPrefix and also uploads
// a metadata file on completion which summarizes the table.
//
// A SHA256 hash of the raw (uncompressed) data is calculated for each part
// and stored with its S3 metadata.
//
// A master-hash is computed for the entire backup by taking the hash from
// each part and concatenating them, in order, joined by newlines (the final
// hash is also terminated with a newline) and then taking the SHA25 hash
// of that document.
//
// Eg. if there are two parts with part one having a hex formatted hash of
// "123abc" and  part two having  a hash of "456def", the hash stored
// in the backup's metadata will be SHA256("123abc\n456def\n").
type S3Writer struct {
	S3          S3Puter
	Bucket      string // S3 bucket name to upload to
	PathPrefix  string // Prefix to apply to each part of the backup
	PartSize    int    // number of bytes to store each part
	MaxParallel int    // Maximum number of parallel uploads to perform to S3

	md              Metadata
	partnum         int32
	rawBytes        int64
	compressedBytes int64
	writeCount      int64
	data            chan []byte // workers read from this channel
	wg              sync.WaitGroup
	fm              sync.Mutex
	failed          error
	metaHash        *hashCalc
	mm              sync.Mutex // metadata mutex
}

// NewS3Writer creates and initializes a new S3Writer
func NewS3Writer(s3 S3Puter, bucket, pathPrefix string, metadata Metadata) *S3Writer {
	metadata.Status = StatusRunning
	metadata.Type = BackupFull
	metadata.StartTime = time.Now()
	metadata.EndTime = nil
	metadata.PartCount = 0
	metadata.UncompressedBytes = 0
	metadata.CompressedBytes = 0
	metadata.ItemCount = 0

	return &S3Writer{
		S3:          s3,
		Bucket:      bucket,
		PathPrefix:  pathPrefix,
		PartSize:    DefaultPartSize,
		MaxParallel: DefaultS3MaxParallel,
		md:          metadata,
		data:        make(chan []byte),
		metaHash:    newHashCalc(),
	}
}

// Run starts goroutines to feed incoming data sent to Write to S3.
func (w *S3Writer) Run() error {
	if w.data == nil {
		w.data = make(chan []byte)
	}
	if w.PartSize < MinPartSize {
		return errors.New("PartSize too small")
	}
	if w.MaxParallel < 1 {
		return errors.New("MaxParallel must be 1 or greater")
	}
	if err := w.flushMetadata(); err != nil {
		return err
	}
	for i := 0; i < w.MaxParallel; i++ {
		w.wg.Add(1)
		go w.worker()
	}
	w.wg.Wait()
	now := time.Now()
	w.md.EndTime = &now
	if err := w.failError(); err != nil {
		w.md.Status = StatusFailed
		w.flushMetadata()
		return err
	}

	w.md.Status = StatusCompleted
	return w.flushMetadata()
}

// Write takes a single block of JSON text and sends it to S3.
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

// Abort closes the writer and marks the metadata state as failed
func (w *S3Writer) Abort() error {
	w.fail(errors.New("aborted"))
	return w.Close()
}

func (w *S3Writer) completePart(pn int, itemCount, uncompressedBytes, compressedBytes int64, partHash hash.Hash) error {
	w.mm.Lock()
	defer w.mm.Unlock()

	w.metaHash.add(pn-1, partHash)
	w.md.UncompressedBytes += uncompressedBytes
	w.md.CompressedBytes += compressedBytes
	w.md.ItemCount += itemCount
	w.md.PartCount++
	n, hstr := w.metaHash.value()
	w.md.LastHashed = n
	w.md.Hash = hstr
	return w.flushMetadata()
}

func (w *S3Writer) flushMetadata() error {
	data, err := json.MarshalIndent(w.md, "", "  ")
	if err != nil {
		return err
	}
	req := &s3.PutObjectInput{
		Bucket:      aws.String(w.Bucket),
		Key:         aws.String(s3MetaKey(w.PathPrefix)),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/json"),
	}
	_, err = w.S3.PutObject(req)
	return err
}

// newKey generates the next S3 object key and part metadata key
func (w *S3Writer) newKey() (partnum int32, path string) {
	pn := atomic.AddInt32(&w.partnum, 1)
	return pn, fmt.Sprintf("%s%09d.json.gz", s3PartPrefix(w.PathPrefix), pn)
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
	var rawPendingLen int64
	var writeCount int64

	defer w.wg.Done()

	tmpfile, err := ioutil.TempFile("", "dyndump")
	if err != nil {
		w.fail(err)
		return
	}
	defer os.Remove(tmpfile.Name())

	// calculate a hash of ungzipped content is written to the output
	fhash := sha256.New()
	gz := gzip.NewWriter(tmpfile)
	writer := io.MultiWriter(gz, fhash)

	flush := func() error {
		if err := w.failError(); err != nil {
			failed = true // complete final flush
		}
		gz.Close()
		fsize, _ := tmpfile.Seek(0, 1)
		tmpfile.Seek(0, 0)

		pn, key := w.newKey()
		fhashStr := fmt.Sprintf("%x", fhash.Sum(nil))
		req := &s3.PutObjectInput{
			Bucket:          aws.String(w.Bucket),
			Key:             aws.String(key),
			Body:            tmpfile,
			ContentEncoding: aws.String("gzip"),
			ContentType:     aws.String("application/json"),
			Metadata: map[string]*string{
				metaSha256:     aws.String(fhashStr),
				metaItemCount:  aws.String(strconv.FormatInt(writeCount, 10)),
				metaPartNumber: aws.String(strconv.FormatInt(int64(pn), 10)),
			},
		}
		_, err := w.S3.PutObject(req)
		if err != nil {
			return err
		}

		if err := w.completePart(int(pn), writeCount, rawPendingLen, fsize, fhash); err != nil {
			return err
		}

		rawPendingLen = 0
		writeCount = 0
		tmpfile.Truncate(0)
		tmpfile.Seek(0, 0)
		fhash.Reset()
		gz.Reset(tmpfile)
		return nil
	}

	var intervalBytes int
	gzipFlushInterval := w.PartSize / 10
	for data := range w.data {
		if failed {
			continue
		}
		writer.Write(data)
		rawPendingLen += int64(len(data))
		writeCount++
		intervalBytes += len(data)
		if intervalBytes >= gzipFlushInterval {
			gz.Flush() // Flush to get a sense of how much data is buffered
			intervalBytes = 0
		}
		if fsize, _ := tmpfile.Seek(0, 1); fsize >= int64(w.PartSize) {
			if err := flush(); err != nil {
				w.fail(err)
				failed = true
			}
		}
	}

	if rawPendingLen > 0 && !failed {
		if err := flush(); err != nil {
			w.fail(err)
		}
	}
}

func s3MetaKey(prefix string) string {
	return prefix + "-meta.json"
}
func s3PartPrefix(prefix string) string {
	return prefix + "-part-"
}
