// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package cmd

import (
	"fmt"
	"io"
	"os"
	"sync/atomic"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gwatts/flagvals"
	cli "github.com/gwatts/mow.cli"
)

const (
	kib = 1 << 10
	mib = 1 << 20
	gib = 1 << 30
	tib = 1 << 40
)

func fmtBytes(bytes int64) string {
	switch {
	case bytes < 0:
		return "unknown"
	case bytes < kib:
		return fmt.Sprintf("%d bytes", bytes)
	case bytes < mib:
		return fmt.Sprintf("%.1f KB", float64(bytes)/kib)
	case bytes < gib:
		return fmt.Sprintf("%.1f MB", float64(bytes)/mib)
	case bytes < tib:
		return fmt.Sprintf("%.1f GB", float64(bytes)/gib)
	default:
		return fmt.Sprintf("%.1f TB", float64(bytes)/tib)
	}
}

type readWatcher struct {
	io.Reader
	bytesRead int64
}

func newReadWatcher(r io.Reader) *readWatcher {
	return &readWatcher{Reader: r}
}

func (r *readWatcher) Read(p []byte) (n int, err error) {
	n, err = r.Reader.Read(p)
	atomic.AddInt64(&r.bytesRead, int64(n))
	return n, err
}

func (r *readWatcher) BytesRead() int64 {
	return atomic.LoadInt64(&r.bytesRead)
}

func fail(format string, a ...interface{}) {
	fmt.Fprintf(os.Stderr, format+"\n", a...)
	cli.Exit(100)
}

type awsServices struct {
	s3  *s3.S3
	dyn *dynamodb.DynamoDB
}

func initAWS(maxRetries *flagvals.RangeInt) *awsServices {
	cfg := aws.NewConfig().WithMaxRetries(int(maxRetries.Value))

	s, err := session.NewSession(cfg)
	if err != nil {
		fail("Failed to create AWS session: %v", err)
	}

	return &awsServices{
		s3:  s3.New(s),
		dyn: dynamodb.New(s),
	}
}
