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
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
	"github.com/gwatts/flagvals"
	cli "github.com/gwatts/mow.cli"
)

const (
	s3ObjectNotFound = "NoSuchKey"
)

func RegisterDumpCommand(app *cli.Cli) {
	app.Command("dump", "Dump a table to file and/or S3", func(cmd *cli.Cmd) {
		cmd.Spec = "[-cmpr] [--filename | --stdout] [(--s3-bucket --s3-prefix)] TABLENAME"
		action := &dumper{
			tableName: cmd.StringArg("TABLENAME", "",
				"Table name to dump from Dynamo"),
			consistentRead: cmd.Bool(cli.BoolOpt{
				Name:   "c consistent-read",
				Value:  false,
				Desc:   "Enable consistent reads (at 2x capacity use)",
				EnvVar: "USE_CONSISTENT",
			}),
			filename: cmd.String(cli.StringOpt{
				Name:   "f filename",
				Value:  "",
				Desc:   "Filename to write data to.  May be combined with --s3-* to store in both locations.",
				EnvVar: "FILENAME",
			}),
			stdout: cmd.Bool(cli.BoolOpt{
				Name:   "stdout",
				Value:  false,
				Desc:   "If true then send the output to stdout",
				EnvVar: "USE_STDOUT",
			}),
			s3BucketName: cmd.String(cli.StringOpt{
				Name:   "s3-bucket",
				Value:  "",
				Desc:   "S3 bucket name to upload to.  May be combined with --filename to store in both locations",
				EnvVar: "S3_BUCKET",
			}),
			s3Prefix: cmd.String(cli.StringOpt{
				Name:   "s3-prefix",
				Value:  "",
				Desc:   `Path prefix to use to store data in S3 (eg. "backups/2016-04-01-12:25-")`,
				EnvVar: "S3_PREFIX",
			}),

			maxRetries:   flagvals.GTEInt(awsMaxRetries, 0),
			maxItems:     flagvals.GTEInt(0, 0),
			parallel:     flagvals.BetweenInt(5, 1, maxParallel),
			readCapacity: flagvals.GTEInt(5, 1),
		}

		cmd.Var(cli.VarOpt{
			Name:   "m maxitems",
			Value:  action.maxItems,
			Desc:   "Maximum number of items to dump.  Set to 0 to process all items in the table",
			EnvVar: "MAXITEMS",
		})
		cmd.Var(cli.VarOpt{
			Name:   "p parallel",
			Value:  action.parallel,
			Desc:   "Number of concurrent channels to open to DynamoDB",
			EnvVar: "MAX_PARALLEL",
		})
		cmd.Var(cli.VarOpt{
			Name:   "r read-capacity",
			Value:  action.readCapacity,
			Desc:   "Average aggregate read capacity to use for scan (set to 0 for unlimited)",
			EnvVar: "READ_CAPACITY",
		})
		cmd.Var(cli.VarOpt{
			Name:   "max-retries",
			Value:  action.maxRetries,
			Desc:   "Maximum number of retry attempts to make with AWS services before failing",
			EnvVar: "AWS_MAX_RETRIES",
		})

		cmd.Before = func() {
			if *action.filename == "" && !*action.stdout && *action.s3BucketName == "" {
				fail("Either --filename/--stdout and/or --s3-bucket and --s3-prefix must be set")
			}
		}

		cmd.Action = actionRunner(cmd, action)
	})
}

type writers struct {
	io.Writer
	fileWriter io.WriteCloser
	s3Writer   *dyndump.S3Writer
	s3RunErr   chan error
	names      []string
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

func (w *writers) String() string {
	return strings.Join(w.names, ",")
}

type dumper struct {
	f          *dyndump.Fetcher
	abortChan  chan struct{}
	tableBytes int64
	startTime  time.Time

	aws       *awsServices
	tableInfo *dynamodb.TableDescription

	// options
	tableName      *string
	consistentRead *bool
	filename       *string
	stdout         *bool
	maxItems       *flagvals.RangeInt
	parallel       *flagvals.RangeInt
	readCapacity   *flagvals.RangeInt
	s3BucketName   *string
	s3Prefix       *string
	maxRetries     *flagvals.RangeInt
}

func (d *dumper) openS3Writer() (*dyndump.S3Writer, error) {
	// check if already exists
	r := dyndump.S3Reader{
		S3:         d.aws.s3,
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
		return nil, fmt.Errorf("error requesting s3 backup metadata: %v", err)
	}

	// metadata wasn't found; ok to continue
	md = &dyndump.Metadata{
		TableName: *d.tableName,
		TableARN:  aws.StringValue(d.tableInfo.TableArn),
	}
	return dyndump.NewS3Writer(d.aws.s3, *d.s3BucketName, *d.s3Prefix, *md), nil
}

func (d *dumper) openWriters() (*writers, error) {
	var fout io.Writer
	ws := new(writers)

	if *d.stdout {
		fout = os.Stdout
		ws.names = append(ws.names, "stdout")

	} else if *d.filename != "" {
		var err error
		fout, err = os.Create(*d.filename)
		if err != nil {
			return nil, fmt.Errorf("Failed to open file for write: %s", err)
		}
		ws.names = append(ws.names, fmt.Sprintf("file://%s", *d.filename))
	}

	if *d.s3BucketName != "" {
		if *d.s3Prefix == "" {
			return nil, errors.New("s3-prefix not set")
		}
		w, err := d.openS3Writer()
		if err != nil {
			return nil, fmt.Errorf("Failed to open S3 for write: %v", err)
		}
		ws.names = append(ws.names, fmt.Sprintf("s3://%s/%s", *d.s3BucketName, *d.s3Prefix))
		ws.s3Writer = w
		ws.s3Writer.MaxParallel = int(d.parallel.Value) // match fetcher parallelism
		ws.s3RunErr = make(chan error)
		if fout != nil {
			// stream to both
			ws.Writer = io.MultiWriter(fout, ws.s3Writer)
		} else {
			ws.Writer = ws.s3Writer
		}
		go func() { ws.s3RunErr <- ws.s3Writer.Run() }()
		return ws, nil
	}

	if fout == nil {
		return nil, errors.New("Either s3-bucket & s3-prefix, or filename must be set")
	}

	// no s3
	ws.Writer = fout
	return ws, nil
}

func (d *dumper) init() error {
	d.aws = initAWS(d.maxRetries)
	resp, err := d.aws.dyn.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: d.tableName,
	})
	if err != nil {
		return err
	}
	d.tableInfo = resp.Table
	return nil
}

func (d *dumper) start(termWriter io.Writer, logger *log.Logger) (done chan error, err error) {
	out, err := d.openWriters()
	if err != nil {
		logger.Print(err.Error())
		fail(err.Error())
	}

	w := dyndump.NewSimpleEncoder(out)

	status := fmt.Sprintf("Beginning scan: table=%q readCapacity=%d "+
		"parallel=%d itemCount=%d totalSize=%s targets=%s",
		*d.tableName, d.readCapacity.Value, d.parallel.Value,
		aws.Int64Value(d.tableInfo.ItemCount), fmtBytes(aws.Int64Value(d.tableInfo.TableSizeBytes)),
		out)

	fmt.Fprintln(termWriter, status)
	logger.Println(status)

	d.f = &dyndump.Fetcher{
		Dyn:            d.aws.dyn,
		TableName:      *d.tableName,
		ConsistentRead: *d.consistentRead,
		MaxParallel:    int(d.parallel.Value),
		MaxItems:       d.maxItems.Value,
		ReadCapacity:   float64(d.readCapacity.Value),
		Writer:         w,
	}

	done = make(chan error, 1)
	d.abortChan = make(chan struct{}, 1)
	d.startTime = time.Now()

	go func() {
		rerr := make(chan error)
		go func() { rerr <- d.f.Run() }()

		select {
		case <-d.abortChan:
			logger.Printf("Aborting table dump table=%s", *d.tableName)
			d.f.Stop()
			<-rerr
			out.Abort()
			logger.Printf("Dump abort completed table=%s", *d.tableName)
			done <- errors.New("Aborted")

		case err := <-rerr:
			if err != nil {
				out.Abort()
				logger.Printf("Dump failed table=%s error=%v", *d.tableName, err)
				done <- err
			} else {
				err = out.Close()
				if err != nil {
					logger.Printf("Dump flush failed table=%s error=%v", *d.tableName, err)
				} else {
					logger.Printf("Dump completed OK table=%s", *d.tableName)
				}
				done <- err
			}
		}
		// log final stats
		logger.Println("Final dump stats", d.formatStats())
	}()

	return done, nil
}

func (d *dumper) formatStats() string {
	stats := d.f.Stats()
	deltaSeconds := float64(time.Since(d.startTime) / time.Second)
	return fmt.Sprintf("table=%s avg_items_sec=%.2f avg_capacity_sec=%.2f total_items_read=%d",
		*d.tableName,
		float64(stats.ItemsRead)/deltaSeconds,
		stats.CapacityUsed/deltaSeconds,
		stats.ItemsRead)
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

func (d *dumper) logProgress(logger *log.Logger) {
	logger.Printf("Dump in progress - current stats %s", d.formatStats())
}

func (d *dumper) abort() {
	d.abortChan <- struct{}{}
}

func (d *dumper) printFinalStats(writers ...io.Writer) {
	finalStats := d.f.Stats()
	deltaSeconds := float64(time.Since(d.startTime) / time.Second)

	for _, w := range writers {
		fmt.Fprintf(w, "Avg items/sec: %.2f\n", float64(finalStats.ItemsRead)/deltaSeconds)
		fmt.Fprintf(w, "Avg capacity/sec: %.2f\n", finalStats.CapacityUsed/deltaSeconds)
		fmt.Fprintln(w, "Total items read: ", finalStats.ItemsRead)
	}
}
