// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

/*
Command dyndump dumps a single DynamoDB table to a JSON formatted output

It supports parallel connections for increased throughput, and rate
limiting to a specified read capacity.

By default the JSON output provides an array of objects, with keys for each
column in the row of the table and values mapping to the values in the row.

Numeric values are returned by DynamoDB as strings, but are converted to
floats unless the -string-nums option is specified.

Specifying -typed changes the values in each object to become an object
with keys of "type" and "value" where "type" may be one of DynamoDB's
supported types:
* binary
* binary-set
* bool
* list
* map
* number
* number-set
* null
* string
* string-set

AWS credentials required to connect to DynamoDB must be passed in using
environment variables:
* AWS_ACCESS_KEY_ID
* AWS_SECRET_KEY
*/
package main

import (
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
)

const (
	maxParallel      = 1000
	statsFrequency   = 2 * time.Second
	defaultSliceSize = 10 * 1024 * 1024 // 10MB
)

var (
	consistentRead   = flag.Bool("consistent-read", false, "Enable consistent reads (at 2x capacity use)")
	includeTypes     = flag.Bool("typed", true, "Include type names in JSON output")
	maxItems         = flag.Int64("maxitems", 0, "Maximum number of items to read.  Set to 0 to read all items")
	numbersAsStrings = flag.Bool("string-nums", false, "Output numbers as exact value strings instead of converting")
	outFilename      = flag.String("target", "-", "Filename to write data to.  Defaults to stdout unless bucket is set")
	parallel         = flag.Int("parallel", 4, "Number of concurrent channels to open to DynamoDB")
	readCapacity     = flag.Int("read-capacity", 5, "Average aggregate read capacity to use for scan (set to 0 for unlimited)")
	silent           = flag.Bool("silent", false, "Don't print progress to stderr")
	tableName        = flag.String("tablename", "", "DynamoDB table name to dump")

	// s3 settings
	bucket   = flag.String("s3-bucket", "", "S3 bucket to upload data to")
	s3prefix = flag.String("s3-prefix", "", "Path prefix to use to store data in S3 (eg. backups/2016-04-01-12:25-")
)

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(100)
}

func usage(err string) {
	fmt.Fprintf(os.Stderr, "Error: "+err+"\n\nUsage:\n")
	flag.PrintDefaults()
	fmt.Fprintln(os.Stderr, "\nSpecify AWS_REGION, AWS_ACCESS_KEY and AWS_SECRET_KEY environment variables as required")
	os.Exit(101)
}

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

func openWriters() io.WriteCloser {
	var fout io.Writer = os.Stdout
	ws := new(writers)

	if *outFilename != "" && *outFilename != "-" {
		var err error
		fout, err = os.Create(*outFilename)
		if err != nil {
			fail("Failed to open file for write: %s", err)
		}
	}

	if *bucket != "" {
		if *s3prefix == "" {
			usage("s3prefix not set")
		}
		ws.s3Writer = dyndump.NewS3Writer(s3.New(session.New()), *bucket, *s3prefix)
		ws.s3Writer.MaxParallel = *parallel // match fetcher parallism
		ws.s3RunErr = make(chan error)
		if fout != os.Stdout {
			// stream to both
			ws.Writer = io.MultiWriter(fout, ws.s3Writer)
		} else {
			ws.Writer = ws.s3Writer
		}
		go func() { ws.s3RunErr <- ws.s3Writer.Run() }()
		return ws
	}

	// no s3
	ws.Writer = fout
	return ws
}

func checkFlags() {
	flag.Parse()
	if *tableName == "" {
		usage("No tablename supplied")
	}
	if *parallel < 1 || *parallel > maxParallel {
		usage("Invalid value for -parallel")
	}
	if *readCapacity < 0 {
		usage("Invalid value for -read-capacity")
	}
}

func getTableInfo(dyn *dynamodb.DynamoDB, tableName string) (*dynamodb.TableDescription, error) {
	resp, err := dyn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: aws.String(tableName),
	})
	if err != nil {
		return nil, err
	}
	return resp.Table, nil
}

func main() {
	checkFlags()

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

	dyn := dynamodb.New(session.New())
	tableInfo, err := getTableInfo(dyn, *tableName)
	if err != nil {
		fail("Failed to get table information: %v", err)
	}

	out := openWriters()
	writer := dyndump.NewJSONItemEncoder(out, *includeTypes, *numbersAsStrings)

	if !*silent {
		fmt.Fprintf(os.Stderr, "Beginning scan: table=%q readCapacity=%d parallel=%d itemCount=%d totalSize=%s\n",
			*tableName, *readCapacity, *parallel,
			aws.Int64Value(tableInfo.ItemCount), fmtBytes(aws.Int64Value(tableInfo.TableSizeBytes)))
	}

	f := dyndump.Fetcher{
		Dyn:            dyn,
		TableName:      *tableName,
		ConsistentRead: *consistentRead,
		MaxParallel:    *parallel,
		MaxItems:       *maxItems,
		ReadCapacity:   float64(*readCapacity),
		Writer:         writer,
	}

	doneChan := make(chan error)

	go func() {
		doneChan <- f.Run()
	}()

	var ticker <-chan time.Time
	if !*silent {
		ticker = time.Tick(statsFrequency)
	}

	lastTime := time.Now()
	startTime := lastTime

	var bar *pb.ProgressBar
	if !*silent {
		bar = pb.New64(aws.Int64Value(tableInfo.TableSizeBytes))
		bar = pb.New64(0)
		bar.ShowSpeed = true
		bar.ManualUpdate = true
		bar.SetUnits(pb.U_BYTES)
		bar.SetMaxWidth(78)
		bar.Start()
		bar.Update()
	}

LOOP:
	for {
		select {
		case <-ticker:
			stats := f.Stats()
			bar.Set64(stats.BytesRead)
			bar.Update()

		case <-sigchan:
			bar.Finish()
			fmt.Fprintf(os.Stderr, "\nAborting..")
			f.Stop()
			<-doneChan
			fmt.Fprintf(os.Stderr, "Aborted.\n")
			break LOOP

		case err := <-doneChan:
			if err != nil {
				fail("Processing failed: %v", err)
			}
			break LOOP
		}
	}
	if bar != nil {
		bar.Finish()
	}

	// close the output writers and wait for errors
	if err := out.Close(); err != nil {
		fail("Failed to write output: %v", err)
	}

	finalStats := f.Stats()
	deltaSeconds := float64(time.Since(startTime) / time.Second)

	if !*silent {
		fmt.Fprintf(os.Stderr, "Avg items/sec: %.2f\n", float64(finalStats.ItemsRead)/deltaSeconds)
		fmt.Fprintf(os.Stderr, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
		fmt.Fprintln(os.Stderr, "Total items read: ", f.Stats().ItemsRead)
	}
}
