// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package main

import (
	"fmt"
	"io"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
)

type loader struct {
	loader    *dyndump.Loader
	r         *readWatcher
	md        dyndump.Metadata
	startTime time.Time
	dyn       *dynamodb.DynamoDB
	tableInfo *dynamodb.TableDescription
	source    string

	// options
	tableName      *string
	allowOverwrite *bool
	filename       *string
	stdin          *bool
	maxItems       *int
	parallel       *int
	writeCapacity  *int
	s3BucketName   *string
	s3Prefix       *string
}

func (ld *loader) init() error {
	ld.dyn = dynamodb.New(session.New())
	resp, err := ld.dyn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: ld.tableName,
	})
	if err != nil {
		return err
	}
	ld.tableInfo = resp.Table

	switch {
	case *ld.stdin:
		ld.r = newReadWatcher(os.Stdin)
		ld.source = "stdin"
		ld.md.UncompressedBytes = -1 // unknown

	case *ld.filename != "":
		f, err := os.Open(*ld.filename)
		if err != nil {
			return fmt.Errorf("Failed to open file for read: %v", err)
		}
		ld.source = *ld.filename
		ld.r = newReadWatcher(f)
		if fi, err := f.Stat(); err == nil {
			ld.md.UncompressedBytes = fi.Size()
		}

	case *ld.s3BucketName != "":
		ld.source = fmt.Sprintf("s3://%s/%s", *ld.s3BucketName, *ld.s3Prefix)
		sr := &dyndump.S3Reader{
			S3:         s3.New(session.New()),
			Bucket:     *ld.s3BucketName,
			PathPrefix: *ld.s3Prefix,
		}
		ld.r = newReadWatcher(sr)
		ld.md, err = sr.Metadata()
		if err != nil {
			fail("Failed to read metadata from S3: %v", err)
		}

	default:
		panic("Either s3-bucket & s3-prefix, or filename must be set")
	}

	return nil
}

func (ld *loader) start(infoWriter io.Writer) (done chan error, err error) {
	var hashKey string
	for _, s := range ld.tableInfo.KeySchema {
		if aws.StringValue(s.KeyType) == "HASH" {
			hashKey = aws.StringValue(s.AttributeName)
		}
	}
	if hashKey == "" {
		fail("Failed to find hash key for table")
	}

	fmt.Fprintf(infoWriter, "Beginning restore: table=%q source=%q writeCapacity=%d parallel=%d totalSize=%s allow-overwrite=%t\n",
		*ld.tableName, ld.source, *ld.writeCapacity, *ld.parallel, fmtBytes(ld.md.UncompressedBytes), *ld.allowOverwrite)

	dynLoader := &dyndump.Loader{
		Dyn:            ld.dyn,
		TableName:      *ld.tableName,
		MaxParallel:    *ld.parallel,
		MaxItems:       int64(*ld.maxItems),
		WriteCapacity:  float64(*ld.writeCapacity),
		Source:         dyndump.NewSimpleDecoder(ld.r),
		HashKey:        hashKey,
		AllowOverwrite: *ld.allowOverwrite,
	}

	ld.loader = dynLoader
	done = make(chan error, 1)
	ld.startTime = time.Now()

	go func() {
		done <- dynLoader.Run()
	}()

	return done, nil
}

func (ld *loader) abort() {
	ld.loader.Stop()
}

func (ld *loader) newProgressBar() *pb.ProgressBar {
	bar := pb.New64(ld.md.UncompressedBytes)
	bar.ShowSpeed = true
	bar.SetUnits(pb.U_BYTES)
	return bar
}

func (ld *loader) updateProgress(bar *pb.ProgressBar) {
	bar.Set64(ld.r.BytesRead())
}

func (ld *loader) printFinalStats(w io.Writer) {
	finalStats := ld.loader.Stats()
	deltaSeconds := float64(time.Since(ld.startTime) / time.Second)

	fmt.Fprintf(w, "Avg items/sec: %.2f\n", float64(finalStats.ItemsWritten)/deltaSeconds)
	fmt.Fprintf(w, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
	fmt.Fprintln(w, "Total items written: ", finalStats.ItemsWritten)
	fmt.Fprintln(w, "Total items skipped: ", finalStats.ItemsSkipped)
}
