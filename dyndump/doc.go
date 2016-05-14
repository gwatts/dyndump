// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

/*
Package dyndump exports and imports an entire DynamoDB table.

It supports parallel connections to the DynamoDB and S3 services for increased
throughput along with rate limiting to a specific read & write capacity.

Items are written to an ItemWriter interface until the table is exhausted,
or the Stop method is called.

It also provides an S3Writer type that can be passed to a Fetcher to stream
received data to an S3 bucket.
*/
package dyndump
