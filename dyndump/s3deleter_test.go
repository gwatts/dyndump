// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// Check that the deleter fetches the metadata correctly
func TestDeleterMetadata(t *testing.T) {
	f := &fakeS3Deleter{
		fakeS3GetLister: fakeMetadataResponder(false),
	}

	d, err := NewS3Deleter(f, "test-bucket", "test-prefix")
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	md := d.Metadata()
	if md.TableName != "table-test-bucket" {
		t.Error("Incorrect table name", md.TableName)
	}
	if md.TableARN != "arn-test-prefix-meta.json" {
		t.Error("Incorrect table arn", md.TableARN)
	}
}

func TestDeleteOK(t *testing.T) {
	var deleted []string

	f := &fakeS3Deleter{
		fakeS3GetLister: &fakeS3GetLister{
			list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
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
							{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 0+(2*i)))},
							{Key: aws.String("test-prefix-ignore-this.json.gz")},
							{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 1+(2*i)))},
						},
					}
					cont := fn(page, i == 1)
					if !cont {
						return nil
					}
				}
				return nil
			},
		},
		del: func(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			for _, obj := range input.Delete.Objects {
				deleted = append(deleted, aws.StringValue(obj.Key))
			}
			return new(s3.DeleteObjectsOutput), nil
		},
	}

	d := &S3Deleter{
		s3:         f,
		bucket:     "test-bucket",
		pathPrefix: "test-prefix",
	}

	err := d.Delete()
	if err != nil {
		t.Fatal("Unexpected error", err)
	}

	var expected []string
	for i := 0; i < 4; i++ {
		expected = append(expected, fmt.Sprintf("test-prefix-part-%09d.json.gz", i))
	}
	expected = append(expected, "test-prefix-meta.json")
	if !reflect.DeepEqual(deleted, expected) {
		t.Error("Incorrect delete keys", deleted)
	}
}

func TestDeleteFailedList(t *testing.T) {
	var called bool
	e := errors.New("Test failure")

	f := &fakeS3Deleter{
		fakeS3GetLister: &fakeS3GetLister{
			list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
				return e
			},
		},
		del: func(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			called = true
			return nil, nil
		},
	}

	d := &S3Deleter{
		s3:         f,
		bucket:     "test-bucket",
		pathPrefix: "test-prefix",
	}

	err := d.Delete()
	if err != e {
		t.Fatal("Incorrect error response received", e)
	}

	if called {
		t.Error("Delete operation was called unexpectedly")
	}
}

func TestDeleteFailedCall(t *testing.T) {
	e := errors.New("Test failure")

	f := &fakeS3Deleter{
		fakeS3GetLister: &fakeS3GetLister{
			list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
				page := &s3.ListObjectsOutput{
					Contents: []*s3.Object{
						{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 0))},
						{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 1))},
					},
				}
				fn(page, true)
				return nil
			},
		},
		del: func(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			return nil, e
		},
	}

	d := &S3Deleter{
		s3:         f,
		bucket:     "test-bucket",
		pathPrefix: "test-prefix",
	}

	err := d.Delete()
	if err == nil {
		t.Fatal("No error response received")
	}
}

func TestDeleteFailedPart(t *testing.T) {
	f := &fakeS3Deleter{
		fakeS3GetLister: &fakeS3GetLister{
			list: func(input *s3.ListObjectsInput, fn func(p *s3.ListObjectsOutput, lastPage bool) (shouldContinue bool)) error {
				page := &s3.ListObjectsOutput{
					Contents: []*s3.Object{
						{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 0))},
						{Key: aws.String(fmt.Sprintf("test-prefix-part-%09d.json.gz", 1))},
					},
				}
				fn(page, true)
				return nil
			},
		},
		del: func(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
			k := input.Delete.Objects[0].Key
			return &s3.DeleteObjectsOutput{
				Errors: []*s3.Error{
					{Code: aws.String("oops"), Key: k, Message: aws.String("delete failed")},
				},
			}, nil
		},
	}

	d := &S3Deleter{
		s3:         f,
		bucket:     "test-bucket",
		pathPrefix: "test-prefix",
	}

	err := d.Delete()
	if err == nil {
		t.Fatal("No error response received")
	}
}

func fakeMetadataResponder(shouldErr bool) *fakeS3GetLister {
	if shouldErr {
		return &fakeS3GetLister{
			get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
				return nil, errors.New("Failed")
			},
		}
	}

	return &fakeS3GetLister{
		get: func(input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			// embed the table name and key in the response for validation
			md := fmt.Sprintf(`{"table_name":"table-%s", "table_arn": "arn-%s"}`,
				aws.StringValue(input.Bucket),
				aws.StringValue(input.Key))

			resp := &s3.GetObjectOutput{
				Body: ioutil.NopCloser(strings.NewReader(md)),
			}
			return resp, nil
		},
	}
}

type fakeS3Deleter struct {
	*fakeS3GetLister
	del func(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

func (s3 *fakeS3Deleter) DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error) {
	return s3.del(input)
}
