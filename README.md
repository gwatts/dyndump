# AWS DynamoDB Table Dump and Restore

[![Build Status](https://travis-ci.org/gwatts/dyndump.svg?branch=master)](https://travis-ci.org/gwatts/dyndump)


This utility performs a full scan of an AWS DynamoDB table and outputs each
row as a JSON object, storing the dump either on disk or in an S3 bucket.

It supports rate-limiting to a specified average read or write capacity and
parallel requests to achieve high throughput.

The underlying Go library can also be imported into other projects to provide
dump/load facilities.

## Download

Binaries are available for Linux, Solaris, Windows and Mac for the
[latest release](https://github.com/gwatts/dyndump/releases).

## Compile

[Install Go](https://golang.org/doc/install) and run
`go get github.com/gwatts/dyndump`.


## Example

```bash
$ ./dyndump dump \
        --log dump.log \
        --s3-bucket dyndump-test \
        --s3-prefix backup-20170404-00/test-table-20170404 \
        --read-capacity 1000 \
        test-table
Beginning scan: table="test-table" readCapacity=1000 parallel=5 itemCount=425397 totalSize=2.5 GB targets=s3://dyndump-test/backup-20170404-00/test-table-20170404
 224.49 MiB / 2.48 GiB [==>-------------------------]   8.83% 6.60 MiB/s 5m50s


$ ./dyndump load \
		--log restore.log \
		--s3-bucket dyndump-test \
		--s3-prefix backup-20170404-00/test-table-20170404
		--write-capacity 2000 \
		test-table-copy
Beginning restore: table="test-table-copy" source="s3://dyndump-test/backup-20170404-00/test-table-20170404" writeCapacity=2000 parallel=5 totalSize=3.4 GB allow-overwrite=false skip-checks=false
 3.41 GiB / 3.41 GiB [=============================]  99.99% 2.44 MiB/s 23m50s
Avg items/sec: 297.48
Avg capacity/sec: 1977.75
Total items written:  425397
Total items skipped:  0
```


## Utility Usage

AWS credentials required to connect to DynamoDB must be passed in using
environment variables, or will be loaded from ~/.aws/credentials or using EC2 metadata.

* `AWS_REGION`
* `AWS_ACCESS_KEY_ID`
* `AWS_SECRET_ACCESS_KEY`

By default, the utility will update the console with dump/restore progress.  To use
as part of a script with log output to stdout instead, use the `--log` and `--no-progress`
options:

```
dyndump dump --no-progress --log=- --filename=dumpfile.json
```

Most options are also accepted as environment variables; eg. `--s3-bucket` can
be set using the `S3_BUCKET` environment variable.

The dyndump program supports four commands:

### Dump

Dumps an entire DynamoDB table to file or an S3 bucket.

```
Usage: dyndump dump [--silent] [--no-progress] [--log] [-cmpr] [--filename | --stdout] [(--s3-bucket --s3-prefix)] TABLENAME

Dump a table to file and/or S3

Arguments:
  TABLENAME=""   Table name to dump from Dynamo

Options:
  -c, --consistent-read=false   Enable consistent reads (at 2x capacity use) ($USE_CONSISTENT)
  -f, --filename=""             Filename to write data to.  May be combined with --s3-* to store in both locations. ($FILENAME)
  --stdout=false                If true then send the output to stdout ($USE_STDOUT)
  --s3-bucket=""                S3 bucket name to upload to.  May be combined with --filename to store in both locations ($S3_BUCKET)
  --s3-prefix=""                Path prefix to use to store data in S3 (eg. "backups/2016-04-01-12:25-") ($S3_PREFIX)
  -m, --maxitems=0              Maximum number of items to dump.  Set to 0 to process all items in the table ($MAXITEMS)
  -p, --parallel=5              Number of concurrent channels to open to DynamoDB ($MAX_PARALLEL)
  -r, --read-capacity=5         Average aggregate read capacity to use for scan (set to 0 for unlimited) ($READ_CAPACITY)
  --max-retries=10              Maximum number of retry attempts to make with AWS services before failing ($AWS_MAX_RETRIES)
  --silent=false                Set to true to disable all non-error and non-log output ($SILENT)
  --no-progress=false           Set to true to disable the progress bar ($NO_PROGRESS)
  --log=""                      Set to a filename or --log=- for stdout; defaults to no log output ($LOG_TARGET)
```

### Load

Loads a previous dump from file or S3 into an existing DynamoDB table

```
Usage: dyndump load [--silent] [--no-progress] [--log] [-mpw] [--allow-overwrite] (--filename | --stdin | (--s3-bucket --s3-prefix)) [--skip-checks] TABLENAME

Load a table dump from S3 or file to a DynamoDB table

Arguments:
  TABLENAME=""   Table name to load into

Options:
  --allow-overwrite=false   Set to true to overwrite any existing rows ($ALLOW_OVERWRITE)
  -f, --filename=""         Filename to read data from.  Set to "-" for stdin ($FILENAME)
  --stdin=false             If true then read the dump data from stdin ($USE_STDIN)
  --s3-bucket=""            S3 bucket name to read from ($S3_BUCKET)
  --s3-prefix=""            Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-") ($S3_PREFIX)
  --skip-checks=false       If true then data integrity checks will be skipped during restore ($SKIP_CHECKS)
  -m, --maxitems=0          Maximum number of items to load.  Set to 0 to process all items in the table ($MAXITEMS)
  -p, --parallel=5          Number of concurrent channels to open to DynamoDB ($MAX_PARALLEL)
  -w, --write-capacity=5    Average aggregate read capacity to use for load (set to 0 for unlimited) ($WRITE_CAPACITY)
  --max-retries=10          Maximum number of retry attempts to make with AWS services before failing ($AWS_MAX_RETRIES)
  --silent=false            Set to true to disable all non-error and non-log output ($SILENT)
  --no-progress=false       Set to true to disable the progress bar ($NO_PROGRESS)
  --log=""                  Set to a filename or --log=- for stdout; defaults to no log output ($LOG_TARGET)
```

### Info

Retrieves and displays metadata about a dump stored in S3

```
Usage: dyndump info --s3-bucket --s3-prefix

Display backup metadata from an S3 backup

Options:
  --s3-bucket=""     S3 bucket name to read from ($S3_BUCKET)
  --s3-prefix=""     Path prefix to use to read data from S3 (eg. "backups/2016-04-01-12:25-") ($S3_PREFIX)
  --max-retries=10   Maximum number of retry attempts to make with AWS services before failing ($AWS_MAX_RETRIES)
```

### Delete

Deletes an entire dump from S3 matching a specified prefix.

```
Usage: dyndump delete [--silent] [--no-progress] [--log] --s3-bucket --s3-prefix [--force]

Delete a backup from S3

Options:
  --s3-bucket=""        S3 bucket name to delete from ($S3_BUCKET)
  --s3-prefix=""        Path prefix to use to delete data from S3 (eg. "backups/2016-04-01-12:25-") ($S3_PREFIX)
  --force=false         Set to true to disable the delete prompt ($NO_DELETE_PROMPT)
  --max-retries=10      Maximum number of retry attempts to make with AWS services before failing ($AWS_MAX_RETRIES)
  --silent=false        Set to true to disable all non-error and non-log output ($SILENT)
  --no-progress=false   Set to true to disable the progress bar ($NO_PROGRESS)
  --log=""              Set to a filename or --log=- for stdout; defaults to no log output ($LOG_TARGET)
```


## Output Format

JSON is emitted as a stream of objects, one per item in the canonical format
used by the DynamoDB API.  Each object has a key for each field name with a
value object holding the type and field value.  Eg

```
  {
          "string-field": {"S": "string value"},
          "number-field": {"N": "123"}
  }
```

The following types are defined by the DynamoDB API:

```
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
```


## Library

See the [godoc documentation](https://godoc.org/github.com/gwatts/dyndump/dyndump)
for the github.com/gwatts/dyndump/dyndump library to integrate the library into
your own projects.
