// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import (
	"errors"
	"fmt"
	"regexp"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3DeleteGetLister defines the portion of hte S3 service required by S3Deleter.
type S3DeleteGetLister interface {
	S3GetLister
	DeleteObjects(input *s3.DeleteObjectsInput) (*s3.DeleteObjectsOutput, error)
}

// S3Deleter deletes all parts of a Dynamo backup from S3.
//
// Given a bucket and path prefix, it will check that the backup has a valid
// metadata file and then remove all of the parts that are associated with it,
// before finally removing the metadata file itself.
type S3Deleter struct {
	s3         S3DeleteGetLister
	bucket     string // bucket is the name of the S3 Bucket to read from
	pathPrefix string // pathPrefix is the prefix used to store the backup
	md         Metadata
	delcount   int64
	abort      int64
}

// NewS3Deleter creates and initializes an S3Deleter.  It will attempt to
// fetch the metadata object from S3 before returning to confirm that a
// valid backup actually exists at the given pathPrefix.
func NewS3Deleter(s3 S3DeleteGetLister, bucket, pathPrefix string) (*S3Deleter, error) {
	r := &S3Reader{
		S3:         s3,
		Bucket:     bucket,
		PathPrefix: pathPrefix,
	}
	md, err := r.Metadata()
	if err != nil {
		return nil, err
	}
	return &S3Deleter{
		s3:         s3,
		bucket:     bucket,
		pathPrefix: pathPrefix,
		md:         md,
	}, nil
}

// Metadata returns the metadata read by NewS3Deleter.
func (d *S3Deleter) Metadata() Metadata {
	return d.md
}

// Completed returns the number of parts that have been deleted from S3 so far.
// It may be called while a delete is in progress.
func (d *S3Deleter) Completed() int64 {
	return atomic.LoadInt64(&d.delcount)
}

// Abort requests the deleter discontinues deleting the backup.
func (d *S3Deleter) Abort() {
	atomic.StoreInt64(&d.abort, 1)
}

// Delete starts deleting the configured backup.  It will block until the
// delete operations complete.
func (d *S3Deleter) Delete() (err error) {
	bucket := aws.String(d.bucket)
	prefix := aws.String(s3PartPrefix(d.pathPrefix))
	isPart, err := regexp.Compile(fmt.Sprintf(`^%s\d{9}.json.gz$`, s3PartPrefix(d.pathPrefix)))
	if err != nil {
		return errors.New("Illegal path prefix")
	}

	req := &s3.ListObjectsInput{
		Bucket: bucket,
		Prefix: prefix,
	}
	mdkey := s3MetaKey(d.pathPrefix)

	isCompleted := false

	s3err := d.s3.ListObjectsPages(req, func(page *s3.ListObjectsOutput, lastPage bool) bool {
		if d.isAborted() {
			return false
		}

		del := &s3.DeleteObjectsInput{
			Bucket: bucket,
			Delete: &s3.Delete{Quiet: aws.Bool(true)},
		}

		for _, value := range page.Contents {
			if !isPart.Match([]byte(aws.StringValue(value.Key))) {
				continue // ignore anything that isn't a part, including metadata
			}
			del.Delete.Objects = append(del.Delete.Objects, &s3.ObjectIdentifier{Key: value.Key})
		}
		if len(del.Delete.Objects) > 0 {
			resp, rerr := d.s3.DeleteObjects(del)
			if rerr != nil {
				err = rerr
				return false
			}
			if errs := resp.Errors; len(errs) > 0 {
				err = fmt.Errorf("Failed to delete key %q: %v",
					aws.StringValue(errs[0].Key),
					aws.StringValue(errs[0].Message))
				return false
			}
			atomic.AddInt64(&d.delcount, int64(len(del.Delete.Objects)))
		}
		if lastPage {
			isCompleted = true
		}
		return !d.isAborted()
	})

	if s3err != nil {
		return s3err
	}

	if err == nil && isCompleted {
		// Delete the metadata file
		del := &s3.DeleteObjectsInput{
			Bucket: bucket,
			Delete: &s3.Delete{
				Quiet:   aws.Bool(true),
				Objects: []*s3.ObjectIdentifier{{Key: aws.String(mdkey)}},
			},
		}
		resp, rerr := d.s3.DeleteObjects(del)
		if rerr != nil {
			return rerr
		}
		if errs := resp.Errors; len(errs) > 0 {
			return fmt.Errorf("Failed to delete key %q: %v",
				aws.StringValue(errs[0].Key),
				aws.StringValue(errs[0].Message))
		}
	}

	return err
}

func (d *S3Deleter) isAborted() bool {
	return atomic.LoadInt64(&d.abort) != 0
}
