// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestS3ReadOK(t *testing.T) {
	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			//once.Do(func() {
			if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
				return fmt.Errorf("incorrect bucket for list %q", bucketName)
			}
			if prefix := aws.StringValue(input.Prefix); prefix != "test-prefix-part-" {
				return fmt.Errorf("incorrect prefix for list %q", prefix)
			}

			// generate a couple of pages
			for i := 0; i < 2; i++ {
				page := &s3.ListObjectsOutput{
					Contents: []*s3.Object{
						{Key: aws.String(fmt.Sprintf("key%d", 0+(2*i)))},
						{Key: aws.String(fmt.Sprintf("key%d", 1+(2*i)))},
					},
				}
				cont := fn(page, i == 1)
				if !cont {
					return nil
				}
			}
			return nil
		},

		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
				return nil, fmt.Errorf("incorrect bucket for get %q", bucketName)
			}
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(strings.NewReader(fmt.Sprintf("get %s\n", aws.StringValue(input.Key)))),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	data, err := ioutil.ReadAll(r)
	if err != nil {
		t.Fatal("Unexpected error", err)
	}
	expected := "get key0\nget key1\nget key2\nget key3\n"
	if s := string(data); s != expected {
		t.Errorf("expected=%q actual=%q", expected, s)
	}
}

// Check that an error response from list objects translates into a read error
func TestS3ReadListFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			return testError
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error resposne", err)
	}
}

// Check that an error from GetObject transaltes into a read error
func TestS3GetFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			page := &s3.ListObjectsOutput{
				Contents: []*s3.Object{
					{Key: aws.String("key00")},
				},
			}
			fn(page, false)
			return nil
		},

		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return nil, testError
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error resposne", err)
	}
}

// Check that a body read failure from S3 is propogated to a read error
func TestS3ReadFailed(t *testing.T) {
	var testError = errors.New("test error")

	f := &fakeS3GetLister{
		list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
			page := &s3.ListObjectsOutput{
				Contents: []*s3.Object{
					{Key: aws.String("key00")},
				},
			}
			fn(page, false)
			return nil
		},

		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(&errReader{content: strings.NewReader("test"), err: testError}),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	_, err := ioutil.ReadAll(r)
	if err != testError {
		t.Error("Incorrect error resposne", err)
	}
}

func TestS3ReadMetadata(t *testing.T) {
	f := &fakeS3GetLister{
		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			if bucketName := aws.StringValue(input.Bucket); bucketName != "test-bucket" {
				return nil, fmt.Errorf("incorrect bucket for list %q", bucketName)
			}
			if k := aws.StringValue(input.Key); k != "test-prefix-meta.json" {
				return nil, errors.New("Incorrect key name " + k)
			}
			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(strings.NewReader(`{"table_name":"a_table","item_count":1234}`)),
			}
			return resp, nil
		},
	}

	r := &S3Reader{
		S3:         f,
		Bucket:     "test-bucket",
		PathPrefix: "test-prefix",
	}

	md, err := r.Metadata()
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	if md.TableName != "a_table" || md.ItemCount != 1234 {
		t.Error("incorrect metadata", md)
	}
}

type fakeS3GetLister struct {
	list func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error
	get  func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

func (s3 *fakeS3GetLister) GetObject(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return s3.get(input)
}

func (s3 *fakeS3GetLister) ListObjectsPages(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
	return s3.list(input, fn)
}

type errReader struct {
	content io.Reader
	err     error
}

func (r *errReader) Read(p []byte) (n int, err error) {
	n, err = r.content.Read(p)
	if err != nil {
		return n, r.err
	}
	return n, err
}
