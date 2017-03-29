// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sync"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

var (
	maxKeys = 1000
)

// S3GetLister defines the portion of the S3 service required by S3Reader.
type S3GetLister interface {
	GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	ListObjectsPages(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error
}

// S3Reader reads raw decompressed data from S3 and exposes it as a single
// byte stream by implementing the io.Reader interface.
type S3Reader struct {
	S3            S3GetLister
	Bucket        string // Bucket is the name of the S3 Bucket to read from
	PathPrefix    string // PathPrefix is the prefix used to store the backup
	currentReader io.ReadCloser
	r             *io.PipeReader
	w             *io.PipeWriter
	err           error
	m             sync.Mutex
	md            *Metadata
}

// Metadata returns the backup's metadata information.
func (r *S3Reader) Metadata() (md *Metadata, err error) {
	r.m.Lock()
	defer r.m.Unlock()
	if r.md != nil {
		return r.md, nil
	}

	mdkey := s3MetaKey(r.PathPrefix)
	req := &s3.GetObjectInput{
		Bucket: aws.String(r.Bucket),
		Key:    aws.String(mdkey),
	}
	resp, err := r.S3.GetObject(req)
	if err != nil {
		return md, err
	}
	err = json.NewDecoder(resp.Body).Decode(&r.md)
	return r.md, err
}

// Read reads a block of data from the backup
// It is not safe to call this concurrently from different goroutines.
func (r *S3Reader) Read(p []byte) (n int, err error) {
	if r.err != nil {
		return 0, err
	}
	if r.r == nil {
		r.r, r.w = io.Pipe()
		md, err := r.Metadata()
		if err != nil {
			return 0, err
		}
		go r.reader(md)
	}
	n, err = r.r.Read(p)
	if err != nil {
		r.err = err
	}
	return n, err
}

// reader is a goroutine started by Read that pulls all of the individual
// backup objects from S3 and sends their data into one half of a pipe
// for aggregate reads by Read.
func (r *S3Reader) reader(md *Metadata) {
	var closed bool
	partHash := sha256.New()
	aggHash := sha256.New() // hash of hashes
	target := io.MultiWriter(r.w, partHash)

	req := &s3.ListObjectsInput{
		Bucket: aws.String(r.Bucket),
		Prefix: aws.String(s3PartPrefix(r.PathPrefix)),
	}
	var partCount int
	err := r.S3.ListObjectsPages(req, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		for _, value := range page.Contents {
			req := &s3.GetObjectInput{
				Bucket: aws.String(r.Bucket),
				Key:    value.Key,
			}
			getResp, err := r.S3.GetObject(req)
			if err != nil {
				r.w.CloseWithError(err)
				closed = true
				return false
			}
			if _, err := io.Copy(target, getResp.Body); err != nil {
				r.w.CloseWithError(err)
				closed = true
				return false
			}
			if metahash := aws.StringValue(getResp.Metadata[metaSha256]); metahash != "" {
				hstr := fmt.Sprintf("%x", partHash.Sum(nil))
				if hstr != metahash {
					r.w.CloseWithError(fmt.Errorf("part %s hash mismatch expected=%s actual=%s",
						value.Key, metahash, hstr))
				}
				fmt.Fprintln(aggHash, hstr)
			}
			partHash.Reset()
			partCount++
		}
		return true
	})

	if closed {
		fmt.Println("Exit on close")
		return
	}

	if err != nil {
		r.w.CloseWithError(err)
		return
	}

	// check that we received as many parts as the metadata says should exist
	if md.PartCount > 0 && partCount != md.PartCount {
		r.w.CloseWithError(fmt.Errorf("incomplete restore; expected %d parts, found %d",
			md.PartCount, partCount))
		return
	}

	// check that the metadata hash matches expected
	hstr := fmt.Sprintf("%x", aggHash.Sum(nil))
	fmt.Println("EXP", md.Hash, "Actual", hstr)
	if md.Hash != "" && md.LastHashed == md.PartCount && hstr != md.Hash {
		fmt.Println("CORRUPT")
		r.w.CloseWithError(fmt.Errorf("corrupt restore; expected final hash of %s, got %s",
			md.Hash, hstr))
		return
	}

	r.w.Close()
}
