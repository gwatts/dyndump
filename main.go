// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

/*
Command dyndump dumps or restores a single DynamoDB table to/from a JSON
formatted file or collection of S3 objects.

It supports parallel connections for increased throughput, and rate
limiting to a specified read or write capacity.

JSON is emitted as a stream of objects, one per item in the canonical format
used by the DynamoDB API.  Each object has a key for each field name with a
value object holding the type and field value.  Eg

  {
	  "string-field": {"S": "string value"},
	  "number-field": {"N": "123"}
  }

The following types are defined by the DynamoDB API:

  * S - String
  * N - Number (encoded in JSON as a string)
  * B - Binary (a base64 encoded string)
  * BOOL - Boolean
  * NULL - Null
  * SS - String set
  * NS - Number set
  * BS - Binary set
  * L - List
  * M - Map


AWS credentials will be read from EC2 metadata, ~/.aws/credentials or from
the following environment variables:

  * AWS_REGION
  * AWS_ACCESS_KEY_ID
  * AWS_SECRET_ACCESS_KEY

Usage:


dyndump supports four commands:


DUMP

  Usage: dyndump dump [--silent] [--no-progress] [-cmpr] [--filename | --stdout] [(--s3-bucket --s3-prefix)] TABLENAME

  Dump a table to file or S3

  Arguments:
    TABLENAME=""   Table name to dump from Dynamo

  Options:
    -c, --consistent-read=false   Enable consistent reads (at 2x capacity use)
    -f, --filename=""             Filename to write data to.
    --stdout=false                If true then send the output to stdout
    -m, --maxitems=0              Maximum number of items to dump.  Set to 0 to process all items
    -p, --parallel=5              Number of concurrent channels to open to DynamoDB
    -r, --read-capacity=5         Average aggregate read capacity to use for scan (set to 0 for unlimited)
    --s3-bucket=""                S3 bucket name to upload to
    --s3-prefix=""                Path prefix to use to store data in S3 (eg. "backups/2016-04-01-12:25-")
    --silent=false                Set to true to disable all non-error output
    --no-progress=false           Set to true to disable the progress bar


LOAD

  Usage: dyndump load [--silent] [--no-progress] [-mpw] (--filename | --stdin | (--s3-bucket --s3-prefix)) TABLENAME

  Load a table dump from S3 or file to a DynamoDB table

  Arguments:
    TABLENAME=""   Table name to load into

  Options:
    --allow-overwrite=false   Set to true to overwrite any existing rows
    -f, --filename=""         Filename to read data from.  Set to "-" for stdin
    --stdin=false             If true then read the dump data from stdin
    -m, --maxitems=0          Maximum number of items to load.  Set to 0 to process all items
    -p, --parallel=4          Number of concurrent channels to open to DynamoDB
    -w, --write-capacity=5    Average aggregate write capacity to use for load (set to 0 for unlimited)
    --s3-bucket=""            S3 bucket name to read from
    --s3-prefix=""            Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-")
    --silent=false            Set to true to disable all non-error output
    --no-progress=false       Set to true to disable the progress bar


INFO

  Usage: dyndump info --s3-bucket --s3-prefix

  Display backup metadata from an S3 backup

  Options:
    --s3-bucket=""   S3 bucket name to read from
    --s3-prefix=""   Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-")


DELETE

  Usage: dyndump delete [--silent] [--no-progress] --s3-bucket --s3-prefix [--force]

  Delete a backup from S3

  Options:
    --s3-bucket=""        S3 bucket name to delete from
    --s3-prefix=""        Path prefix to use to delete from S3 (eg. "backups/2016-04-01-12:25-")
    --force=false         Set to true to disable the delete prompt
    --silent=false        Set to true to disable all non-error output
    --no-progress=false   Set to true to disable the progress bar
*/
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/jawher/mow.cli"
)

const (
	maxParallel    = 1000
	statsFrequency = 2 * time.Second
)

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	cli.Exit(100)
}

func checkGTE(value, min int, flag string) {
	if value < min {
		fail("%s must be %d or greater", flag, min)
	}
}

func checkLTE(value, max int, flag string) {
	if value > max {
		fail("%s must be %d or less", flag, max)
	}
}

func main() {
	app := cli.App("dyndump", "Dump and restore DynamoDB database tables")
	app.LongDesc = "long desc goes here"

	app.Command("dump", "Dump a table to file or S3", func(cmd *cli.Cmd) {
		cmd.Spec = "[-cmpr] [--filename | --stdout] [(--s3-bucket --s3-prefix)] TABLENAME"
		action := &dumper{
			tableName:      cmd.StringArg("TABLENAME", "", "Table name to dump from Dynamo"),
			consistentRead: cmd.BoolOpt("c consistent-read", false, "Enable consistent reads (at 2x capacity use)"),
			filename:       cmd.StringOpt("f filename", "", "Filename to write data to."),
			stdout:         cmd.BoolOpt("stdout", false, "If true then send the output to stdout"),
			maxItems:       cmd.IntOpt("m maxitems", 0, "Maximum number of items to dump.  Set to 0 to process all items"),
			parallel:       cmd.IntOpt("p parallel", 5, "Number of concurrent channels to open to DynamoDB"),
			readCapacity:   cmd.IntOpt("r read-capacity", 5, "Average aggregate read capacity to use for scan (set to 0 for unlimited)"),
			s3BucketName:   cmd.StringOpt("s3-bucket", "", "S3 bucket name to upload to"),
			s3Prefix:       cmd.StringOpt("s3-prefix", "", `Path prefix to use to store data in S3 (eg. "backups/2016-04-01-12:25-")`),
		}

		cmd.Before = func() {
			checkGTE(*action.parallel, 1, "--parallel")
			checkLTE(*action.parallel, maxParallel, "--parallel")
			checkGTE(*action.maxItems, 0, "--max-items")
			checkGTE(*action.readCapacity, 0, "--read-capacity")
			if *action.filename == "" && !*action.stdout && *action.s3BucketName == "" {
				fail("Either --filename/--stdout and/or --s3-bucket and --s3-prefix must be set")
			}
		}

		cmd.Action = actionRunner(cmd, action)
	})

	app.Command("load", "Load a table dump from S3 or file to a DynamoDB table", func(cmd *cli.Cmd) {
		cmd.Spec = "[-mpw] [--allow-overwrite] (--filename | --stdin | (--s3-bucket --s3-prefix)) TABLENAME"
		action := &loader{
			tableName:      cmd.StringArg("TABLENAME", "", "Table name to load into"),
			allowOverwrite: cmd.BoolOpt("allow-overwrite", false, "Set to true to overwrite any existing rows"),
			filename:       cmd.StringOpt("f filename", "", "Filename to read data from.  Set to \"-\" for stdin"),
			stdin:          cmd.BoolOpt("stdin", false, "If true then read the dump data from stdin"),
			maxItems:       cmd.IntOpt("m maxitems", 0, "Maximum number of items to load.  Set to 0 to process all items"),
			parallel:       cmd.IntOpt("p parallel", 4, "Number of concurrent channels to open to DynamoDB"),
			writeCapacity:  cmd.IntOpt("w write-capacity", 5, "Average aggregate write capacity to use for load (set to 0 for unlimited)"),
			s3BucketName:   cmd.StringOpt("s3-bucket", "", "S3 bucket name to read from"),
			s3Prefix:       cmd.StringOpt("s3-prefix", "", `Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-")`),
		}

		cmd.Before = func() {
			checkGTE(*action.parallel, 1, "--parallel")
			checkLTE(*action.parallel, maxParallel, "--parallel")
			checkGTE(*action.maxItems, 0, "--max-items")
			checkGTE(*action.writeCapacity, 0, "--write-capacity")
		}

		cmd.Action = actionRunner(cmd, action)
	})

	app.Command("info", "Display backup metadata from an S3 backup", func(cmd *cli.Cmd) {
		cmd.Spec = "--s3-bucket --s3-prefix"
		action := &metadataDumper{
			s3BucketName: cmd.StringOpt("s3-bucket", "", "S3 bucket name to read from"),
			s3Prefix:     cmd.StringOpt("s3-prefix", "", `Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-")`),
		}
		cmd.Action = action.run
	})

	app.Command("delete", "Delete a backup from S3", func(cmd *cli.Cmd) {
		cmd.Spec = "--s3-bucket --s3-prefix [--force]"
		action := &deleter{
			s3BucketName: cmd.StringOpt("s3-bucket", "", "S3 bucket name to delete from"),
			s3Prefix:     cmd.StringOpt("s3-prefix", "", `Path prefix to use to delete from S3 (eg. "backups/2016-04-01-12:25-")`),
			force:        cmd.BoolOpt("force", false, "Set to true to disable the delete prompt"),
		}

		cmd.Action = actionRunner(cmd, action)
	})

	app.Run(os.Args)
}
