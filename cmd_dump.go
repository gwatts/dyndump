// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package main

import (
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gwatts/dyndump/dyndump"
	"gopkg.in/cheggaaa/pb.v1"
)

const (
	s3ObjectNotFound = "NoSuchKey"
)

type writers struct {
	io.Writer
	fileWriter io.WriteCloser
	s3Writer   *dyndump.S3Writer
	s3RunErr   chan error
}

func (w *writers) Close() error {
	if w.fileWriter != nil {
		if w, ok := w.Writer.(io.Closer); ok {
			w.Close()
		}
	}
	if w.s3Writer != nil {
		if err := w.s3Writer.Close(); err != nil {
			return err
		}
		return <-w.s3RunErr
	}
	return nil
}

func (w *writers) Abort() {
	if w.fileWriter != nil {
		if w, ok := w.Writer.(io.Closer); ok {
			w.Close()
		}
	}
	if w.s3Writer != nil {
		w.s3Writer.Abort()
		<-w.s3RunErr
	}
}

type dumper struct {
	f          *dyndump.Fetcher
	abortChan  chan struct{}
	tableBytes int64
	startTime  time.Time

	dyn       *dynamodb.DynamoDB
	tableInfo *dynamodb.TableDescription

	// options
	tableName      *string
	consistentRead *bool
	filename       *string
	stdout         *bool
	maxItems       *int
	parallel       *int
	readCapacity   *int
	s3BucketName   *string
	s3Prefix       *string
}

func (d *dumper) openS3Writer() (*dyndump.S3Writer, error) {
	// check if already exists
	svc := s3.New(session.New())
	r := dyndump.S3Reader{
		S3:         svc,
		Bucket:     *d.s3BucketName,
		PathPrefix: *d.s3Prefix,
	}
	md, err := r.Metadata()
	if err == nil {
		// no error; successfully pulled existing metadata
		return nil, fmt.Errorf("backup already exists for path prefix=%q table_name=%q",
			*d.s3Prefix, md.TableName)
	}
	if aerr, ok := err.(awserr.Error); !ok || (ok && aerr.Code() != s3ObjectNotFound) {
		return nil, err
	}

	// metadata wasn't found; ok to continue
	md = dyndump.Metadata{
		TableName: *d.tableName,
		TableARN:  aws.StringValue(d.tableInfo.TableArn),
	}
	return dyndump.NewS3Writer(svc, *d.s3BucketName, *d.s3Prefix, md), nil
}

func (d *dumper) openWriters() *writers {
	var fout io.Writer
	ws := new(writers)

	if *d.stdout {
		fout = os.Stdout

	} else if *d.filename != "" {
		var err error
		fout, err = os.Create(*d.filename)
		if err != nil {
			fail("Failed to open file for write: %s", err)
		}
	}

	if *d.s3BucketName != "" {
		if *d.s3Prefix == "" {
			fail("s3-prefix not set")
		}
		w, err := d.openS3Writer()
		if err != nil {
			fail("Failed: %v", err)
		}
		ws.s3Writer = w
		ws.s3Writer.MaxParallel = *d.parallel // match fetcher parallelism
		ws.s3RunErr = make(chan error)
		if fout != nil {
			// stream to both
			ws.Writer = io.MultiWriter(fout, ws.s3Writer)
		} else {
			ws.Writer = ws.s3Writer
		}
		go func() { ws.s3RunErr <- ws.s3Writer.Run() }()
		return ws
	}

	if fout == nil {
		fail("Either s3-bucket & s3-prefix, or filename must be set")
	}

	// no s3
	ws.Writer = fout
	return ws
}

func (d *dumper) init() error {
	d.dyn = dynamodb.New(session.New())
	resp, err := d.dyn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: d.tableName,
	})
	if err != nil {
		return err
	}
	d.tableInfo = resp.Table
	return nil
}

func (d *dumper) start(infoWriter io.Writer) (done chan error, err error) {
	out := d.openWriters()
	w := dyndump.NewSimpleEncoder(out)

	fmt.Fprintf(infoWriter, "Beginning scan: table=%q readCapacity=%d parallel=%d itemCount=%d totalSize=%s\n",
		*d.tableName, *d.readCapacity, *d.parallel,
		aws.Int64Value(d.tableInfo.ItemCount), fmtBytes(aws.Int64Value(d.tableInfo.TableSizeBytes)))

	d.f = &dyndump.Fetcher{
		Dyn:            d.dyn,
		TableName:      *d.tableName,
		ConsistentRead: *d.consistentRead,
		MaxParallel:    *d.parallel,
		MaxItems:       int64(*d.maxItems),
		ReadCapacity:   float64(*d.readCapacity),
		Writer:         w,
	}

	done = make(chan error)
	d.abortChan = make(chan struct{}, 1)
	d.startTime = time.Now()

	go func() {
		rerr := make(chan error)
		go func() { rerr <- d.f.Run() }()

		select {
		case <-d.abortChan:
			d.f.Stop()
			<-rerr
			out.Abort()
			done <- errors.New("Aborted")

		case err := <-rerr:
			if err != nil {
				out.Abort()
				done <- err
			} else {
				done <- out.Close()
			}
		}
	}()

	return done, nil
}

func (d *dumper) newProgressBar() *pb.ProgressBar {
	bar := pb.New64(aws.Int64Value(d.tableInfo.TableSizeBytes))
	bar.ShowSpeed = true
	bar.SetUnits(pb.U_BYTES)
	return bar
}

func (d *dumper) updateProgress(bar *pb.ProgressBar) {
	bar.Set64(d.f.Stats().BytesRead)
}

func (d *dumper) abort() {
	d.abortChan <- struct{}{}
}

func (d *dumper) printFinalStats(w io.Writer) {
	finalStats := d.f.Stats()
	deltaSeconds := float64(time.Since(d.startTime) / time.Second)

	fmt.Fprintf(w, "Avg items/sec: %.2f\n", float64(finalStats.ItemsRead)/deltaSeconds)
	fmt.Fprintf(w, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
	fmt.Fprintln(w, "Total items read: ", finalStats.ItemsRead)
}
