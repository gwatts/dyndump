// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package main

import (
	"errors"
	"fmt"
	"io"

	"github.com/Bowery/prompt"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
)

type deleter struct {
	del *dyndump.S3Deleter

	// options
	force        *bool
	s3BucketName *string
	s3Prefix     *string
}

func (d *deleter) init() error {
	del, err := dyndump.NewS3Deleter(s3.New(session.New()), *d.s3BucketName, *d.s3Prefix)
	if err != nil {
		return err
	}
	if !*d.force {
		fmt.Printf("Delete backup of table %s from s3://%s/%s\n\n", del.Metadata().TableName, *d.s3BucketName, *d.s3Prefix)
		ok, err := prompt.Ask("Are you sure you wish to delete the above backup")

		if err != nil {
			return fmt.Errorf("Could not prompt for confirmation (use --force to override): %v", err)
		}
		if !ok {
			return errors.New("User rejected delete")
		}
	}

	d.del = del
	return nil
}

func (d *deleter) start(infoWriter io.Writer) (done chan error, err error) {
	fmt.Fprintf(infoWriter, "Beginning s3 delete prefix=s3://%s/%s parts=%d\n",
		*d.s3BucketName, *d.s3Prefix, d.del.Metadata().PartCount)

	done = make(chan error)

	go func() {
		done <- d.del.Delete()
	}()

	return done, nil
}

func (d *deleter) newProgressBar() *pb.ProgressBar {
	bar := pb.New64(d.del.Metadata().PartCount)
	return bar
}

func (d *deleter) updateProgress(bar *pb.ProgressBar) {
	bar.Set64(d.del.Completed())
}

func (d *deleter) abort() {
	d.del.Abort()
}

func (d *deleter) printFinalStats(w io.Writer) {
	fmt.Fprintf(w, "Deleted %d parts from s3://%s/%s\n",
		d.del.Completed(), *d.s3BucketName, *d.s3Prefix)
}
