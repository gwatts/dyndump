// Copyright 2015 Gareth Watts
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
*list
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
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/gwatts/dyndump/dyndump"
)

const (
	maxParallel    = 1000
	statsFrequency = 2 * time.Second
)

var (
	consistentRead   = flag.Bool("consistent-read", false, "Enable consistent reads (at 2x capacity use)")
	includeTypes     = flag.Bool("typed", true, "Include type names in JSON output")
	maxItems         = flag.Int64("maxitems", 0, "Maximum number of items to read.  Set to 0 to read all items")
	numbersAsStrings = flag.Bool("string-nums", false, "Output numbers as exact value strings instead of converting")
	outFilename      = flag.String("target", "-", "Filename to write data to.  Defaults to stdout")
	parallel         = flag.Int("parallel", 4, "Number of concurrent channels to open to DynamoDB")
	readCapacity     = flag.Int("read-capacity", 5, "Average aggregate read capacity to use for scan (set to 0 for unlimited)")
	region           = flag.String("region", "us-west-2", "AWS Region")
	silent           = flag.Bool("silent", false, "Don't print progress to stderr")
	tableName        = flag.String("tablename", "", "DynamoDB table name to dump")
)

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	os.Exit(100)
}

func usage(err string) {
	fmt.Fprintf(os.Stderr, "Error: "+err+"\n\nUsage:\n")
	flag.PrintDefaults()
	os.Exit(101)
}

func main() {
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

	config := &aws.Config{
		Credentials: credentials.NewEnvCredentials(),
		Region:      aws.String(*region),
	}

	var out io.Writer = os.Stdout
	if *outFilename != "" && *outFilename != "-" {
		var err error
		out, err = os.Create(*outFilename)
		if err != nil {
			fail("Failed to open file for write: %s", err)
		}
	}

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGTERM, syscall.SIGKILL, syscall.SIGINT)

	writer := dyndump.NewJSONItemEncoder(out, *includeTypes, *numbersAsStrings)

	if !*silent {
		fmt.Fprintf(os.Stderr, "Beginning scan: table=%q readCapacity=%d parallel=%d\n",
			*tableName, *readCapacity, *parallel)
	}

	f := dyndump.Fetcher{
		Dyn:            dynamodb.New(config),
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
	var lastStats dyndump.Stats

LOOP:
	for {
		select {
		case <-ticker:
			deltaTime := time.Since(lastTime)
			lastTime = time.Now()
			stats := f.Stats()
			itemsDelta := float64(stats.ItemsRead-lastStats.ItemsRead) / float64(deltaTime/time.Second)
			capacityDelta := (stats.CapacityUsed - lastStats.CapacityUsed) / float64(deltaTime/time.Second)
			lastStats = stats
			fmt.Fprintf(os.Stderr, "Total items: %-10d Items/sec: %-10.1f  Capacity: %-6.1f\n",
				stats.ItemsRead, itemsDelta, capacityDelta)

		case <-sigchan:
			fmt.Fprintf(os.Stderr, "\nAborting..")
			f.Stop()
			<-doneChan
			fmt.Fprintf(os.Stderr, "Aborted.\n")
			break LOOP

		case err := <-doneChan:
			if err != nil {
				fail("Processing failed: %s", err)
			}
			break LOOP
		}
	}

	finalStats := f.Stats()
	deltaSeconds := float64(time.Since(startTime) / time.Second)

	if !*silent {
		fmt.Fprintf(os.Stderr, "Avg items/sec: %.2f\n", float64(finalStats.ItemsRead)/deltaSeconds)
		fmt.Fprintf(os.Stderr, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
		fmt.Fprintln(os.Stderr, "Total items read: ", f.Stats().ItemsRead)
	}
}
