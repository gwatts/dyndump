// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
	"github.com/gwatts/flagvals"
	cli "github.com/gwatts/mow.cli"
)

func RegisterLoadCommand(app *cli.Cli) {
	app.Command("load", "Load a table dump from S3 or file to a DynamoDB table", func(cmd *cli.Cmd) {
		cmd.Spec = "[-mpw] [--allow-overwrite] (--filename | --stdin | (--s3-bucket --s3-prefix)) [--skip-checks] TABLENAME"
		action := &loader{
			tableName: cmd.StringArg("TABLENAME", "",
				"Table name to load into"),
			allowOverwrite: cmd.Bool(cli.BoolOpt{
				Name:   "allow-overwrite",
				Value:  false,
				Desc:   "Set to true to overwrite any existing rows",
				EnvVar: "ALLOW_OVERWRITE",
			}),
			filename: cmd.String(cli.StringOpt{
				Name:   "f filename",
				Value:  "",
				Desc:   "Filename to read data from.  Set to \"-\" for stdin",
				EnvVar: "FILENAME",
			}),
			stdin: cmd.Bool(cli.BoolOpt{
				Name:   "stdin",
				Value:  false,
				Desc:   "If true then read the dump data from stdin",
				EnvVar: "USE_STDIN",
			}),
			s3BucketName: cmd.String(cli.StringOpt{
				Name:   "s3-bucket",
				Value:  "",
				Desc:   "S3 bucket name to read from",
				EnvVar: "S3_BUCKET",
			}),
			s3Prefix: cmd.String(cli.StringOpt{
				Name:   "s3-prefix",
				Value:  "",
				Desc:   `Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-")`,
				EnvVar: "S3_PREFIX",
			}),
			skipIntegrityCheck: cmd.Bool(cli.BoolOpt{
				Name:   "skip-checks",
				Value:  false,
				Desc:   "If true then data integrity checks will be skipped during restore",
				EnvVar: "SKIP_CHECKS",
			}),

			maxRetries:    flagvals.GTEInt(awsMaxRetries, 0),
			maxItems:      flagvals.GTEInt(0, 0),
			parallel:      flagvals.BetweenInt(5, 1, maxParallel),
			writeCapacity: flagvals.GTEInt(5, 1),
		}

		cmd.Var(cli.VarOpt{
			Name:   "m maxitems",
			Value:  action.maxItems,
			Desc:   "Maximum number of items to load.  Set to 0 to process all items in the table",
			EnvVar: "MAXITEMS",
		})
		cmd.Var(cli.VarOpt{
			Name:   "p parallel",
			Value:  action.parallel,
			Desc:   "Number of concurrent channels to open to DynamoDB",
			EnvVar: "MAX_PARALLEL",
		})
		cmd.Var(cli.VarOpt{
			Name:   "w write-capacity",
			Value:  action.writeCapacity,
			Desc:   "Average aggregate read capacity to use for load (set to 0 for unlimited)",
			EnvVar: "WRITE_CAPACITY",
		})
		cmd.Var(cli.VarOpt{
			Name:   "max-retries",
			Value:  action.maxRetries,
			Desc:   "Maximum number of retry attempts to make with AWS services before failing",
			EnvVar: "AWS_MAX_RETRIES",
		})

		cmd.Action = actionRunner(cmd, action)
	})
}

type loader struct {
	loader    *dyndump.Loader
	abortChan chan struct{}
	r         *readWatcher
	md        *dyndump.Metadata
	startTime time.Time
	//dyn        *dynamodb.DynamoDB
	tableInfo *dynamodb.TableDescription
	source    string
	//awsSession *session.Session
	aws *awsServices

	// options
	tableName          *string
	allowOverwrite     *bool
	filename           *string
	stdin              *bool
	maxItems           *flagvals.RangeInt
	parallel           *flagvals.RangeInt
	writeCapacity      *flagvals.RangeInt
	s3BucketName       *string
	s3Prefix           *string
	skipIntegrityCheck *bool
	maxRetries         *flagvals.RangeInt
}

func (ld *loader) init() error {
	ld.aws = initAWS(ld.maxRetries)
	resp, err := ld.aws.dyn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: ld.tableName,
	})
	if err != nil {
		return err
	}
	ld.tableInfo = resp.Table
	ld.md = new(dyndump.Metadata)

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
			S3:                 ld.aws.s3,
			Bucket:             *ld.s3BucketName,
			PathPrefix:         *ld.s3Prefix,
			SkipIntegrityCheck: *ld.skipIntegrityCheck,
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

func (ld *loader) start(termWriter io.Writer, logger *log.Logger) (done chan error, err error) {
	var hashKey string
	for _, s := range ld.tableInfo.KeySchema {
		if aws.StringValue(s.KeyType) == "HASH" {
			hashKey = aws.StringValue(s.AttributeName)
		}
	}
	if hashKey == "" {
		fail("Failed to find hash key for table")
	}

	status := fmt.Sprintf(
		"Beginning restore: table=%q source=%q writeCapacity=%d "+
			"parallel=%d totalSize=%s allow-overwrite=%t skip-checks=%t",
		*ld.tableName, ld.source, ld.writeCapacity.Value,
		ld.parallel.Value, fmtBytes(ld.md.UncompressedBytes), *ld.allowOverwrite, *ld.skipIntegrityCheck)

	fmt.Fprintln(termWriter, status)
	logger.Println(status)

	ld.loader = &dyndump.Loader{
		Dyn:            ld.aws.dyn,
		TableName:      *ld.tableName,
		MaxParallel:    int(ld.parallel.Value),
		MaxItems:       ld.maxItems.Value,
		WriteCapacity:  float64(ld.writeCapacity.Value),
		Source:         dyndump.NewSimpleDecoder(ld.r),
		HashKey:        hashKey,
		AllowOverwrite: *ld.allowOverwrite,
	}

	done = make(chan error, 1)
	ld.abortChan = make(chan struct{}, 1)
	ld.startTime = time.Now()

	go func() {
		rerr := make(chan error)
		go func() { rerr <- ld.loader.Run() }()

		select {
		case <-ld.abortChan:
			logger.Printf("Aborting table load table=%s", *ld.tableName)
			ld.loader.Stop()
			<-rerr
			logger.Printf("Load abort completed table=%s", *ld.tableName)
			done <- errors.New("Aborted")

		case err := <-rerr:
			if err != nil {
				logger.Printf("Restore failed table=%s error=%v", *ld.tableName, err)
				done <- err
			} else {
				logger.Printf("Restore completed OK table=%s", *ld.tableName)
				done <- err
			}
		}
		// log final stats
		logger.Println("Final restore stats", ld.formatStats())
	}()

	return done, nil
}

func (ld *loader) formatStats() string {
	stats := ld.loader.Stats()
	deltaSeconds := float64(time.Since(ld.startTime) / time.Second)
	return fmt.Sprintf("table=%s avg_items_sec=%.2f avg_capacity_sec=%.2f "+
		"total_items_written=%d total_items_skipped=%d",
		*ld.tableName,
		float64(stats.ItemsWritten)/deltaSeconds, stats.CapacityUsed/deltaSeconds,
		stats.ItemsWritten, stats.ItemsSkipped)
}

func (ld *loader) abort() {
	ld.abortChan <- struct{}{}
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

func (ld *loader) logProgress(logger *log.Logger) {
	logger.Printf("Restore in progress - current stats %s", ld.formatStats())
}

func (ld *loader) printFinalStats(w io.Writer) {
	finalStats := ld.loader.Stats()
	deltaSeconds := float64(time.Since(ld.startTime) / time.Second)

	fmt.Fprintf(w, "Avg items/sec: %.2f\n", float64(finalStats.ItemsWritten)/deltaSeconds)
	fmt.Fprintf(w, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
	fmt.Fprintln(w, "Total items written: ", finalStats.ItemsWritten)
	fmt.Fprintln(w, "Total items skipped: ", finalStats.ItemsSkipped)
}
