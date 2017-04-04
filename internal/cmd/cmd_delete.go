// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package cmd

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/Bowery/prompt"
	"github.com/cheggaaa/pb"
	"github.com/gwatts/dyndump/dyndump"
	"github.com/gwatts/flagvals"
	cli "github.com/jawher/mow.cli"
)

func RegisterDeleteCommand(app *cli.Cli) {
	app.Command("delete", "Delete a backup from S3", func(cmd *cli.Cmd) {
		cmd.Spec = "--s3-bucket --s3-prefix [--force]"
		action := &deleter{
			s3BucketName: cmd.String(cli.StringOpt{
				Name:   "s3-bucket",
				Value:  "",
				Desc:   "S3 bucket name to delete from",
				EnvVar: "S3_BUCKET",
			}),
			s3Prefix: cmd.String(cli.StringOpt{
				Name:   "s3-prefix",
				Value:  "",
				Desc:   `Path prefix to use to delete data from S3 (eg. "backups/2016-04-01-12:25-")`,
				EnvVar: "S3_PREFIX",
			}),
			force: cmd.Bool(cli.BoolOpt{
				Name:   "force",
				Value:  false,
				Desc:   "Set to true to disable the delete prompt",
				EnvVar: "NO_DELETE_PROMPT",
			}),

			maxRetries: flagvals.GTEInt(awsMaxRetries, 0),
		}

		cmd.Var(cli.VarOpt{
			Name:   "max-retries",
			Value:  action.maxRetries,
			Desc:   "Maximum number of retry attempts to make with AWS services before failing",
			EnvVar: "AWS_MAX_RETRIES",
		})

		cmd.Action = actionRunner(cmd, action)
	})
}

type deleter struct {
	del    *dyndump.S3Deleter
	s3Path string

	// options
	force        *bool
	s3BucketName *string
	s3Prefix     *string
	maxRetries   *flagvals.RangeInt
}

func (d *deleter) init() error {
	aws := initAWS(d.maxRetries)
	del, err := dyndump.NewS3Deleter(aws.s3, *d.s3BucketName, *d.s3Prefix)
	if err != nil {
		return err
	}
	d.s3Path = fmt.Sprintf("s3://%s/%s", *d.s3BucketName, *d.s3Prefix)

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

func (d *deleter) start(termWriter io.Writer, logger *log.Logger) (done chan error, err error) {

	status := fmt.Sprintf("Beginning s3 delete prefix=%s parts=%d\n",
		d.s3Path, d.del.Metadata().PartCount)

	fmt.Fprintln(termWriter, status)
	logger.Println(status)

	done = make(chan error, 1)

	go func() {
		if err := d.del.Delete(); err != nil {
			logger.Printf("Delete failed prefix=%s error=%v", d.s3Path, err)
		} else {
			logger.Printf("Delete completed OK prefix=%s", d.s3Path)
		}
		logger.Println("Final delete stats", d.formatStats())
		done <- err
	}()

	return done, nil
}

func (d *deleter) formatStats() string {
	return fmt.Sprintf("prefix=%s parts_deleted=%d",
		d.s3Path, d.del.Completed())
}

func (d *deleter) newProgressBar() *pb.ProgressBar {
	bar := pb.New64(int64(d.del.Metadata().PartCount))
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
