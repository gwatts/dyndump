# AWS DynamoDB Table Dump 

[![Build Status](https://travis-ci.org/gwatts/dyndump.svg?branch=master)](https://travis-ci.org/gwatts/dyndump)


This utility performs a full scan of an AWS DynamoDB table and outputs each
row as a JSON object.

It supports rate-limiting to a specified average read capacity and parallel
requests to achieve high throughput.

The underlying Go library can also be imported into other projects to provide
scan facilities.

## Download

Binaries are available for Linux, Solaris, Windows and Mac for the
[latest release](https://github.com/gwatts/dyndump/releases).

## Utility Usage


```
Usage of dyndump:
  -consistent-read
        Enable consistent reads (at 2x capacity use)
  -maxitems int
        Maximum number of items to read.  Set to 0 to read all items
  -parallel int
        Number of concurrent channels to open to DynamoDB (default 4)
  -read-capacity int
        Average aggregate read capacity to use for scan (set to 0 for unlimited) (default 5)
  -region string
        AWS Region (default "us-west-2")
  -silent
        Don't print progress to stderr
  -string-nums
        Output numbers as exact value strings instead of converting
  -tablename string
        DynamoDB table name to dump
  -target string
        Filename to write data to.  Defaults to stdout (default "-")
  -typed
        Include type names in JSON output (default true)
```

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

## dyndump library

See the [godoc documentation](https://godoc.org/github.com/gwatts/dyndump/dyndump)
for the github.com/gwatts/dyndump/dyndump library to integrate scanning into
your own projects.
