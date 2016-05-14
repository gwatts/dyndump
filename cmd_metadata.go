// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package main

import (
	"html/template"
	"os"

	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/gwatts/dyndump/dyndump"
)

var metadataTmpl = template.Must(template.New("md").Parse(`
Table Name...........: {{ .TableName }}
Table ARN............: {{ .TableARN }}
Status ..............: {{ .Status }}
Backup Type .........: {{ .Type }}
Backup Start Time ...: {{ .StartTime }}
Backup End Time .....: {{ .EndTime }}
Compressed (bytes) ..: {{ .CompressedBytes }}
Uncompressed (bytes) : {{ .UncompressedBytes }}
Item Count ..........: {{ .ItemCount }}
Part Count ..........: {{ .PartCount }}
`))

type metadataDumper struct {
	// options
	s3BucketName *string
	s3Prefix     *string
}

func (md *metadataDumper) run() {
	sr := &dyndump.S3Reader{
		S3:         s3.New(session.New()),
		Bucket:     *md.s3BucketName,
		PathPrefix: *md.s3Prefix,
	}
	metadata, err := sr.Metadata()
	if err != nil {
		fail("Failed to read metadata from S3: %v", err)
	}
	metadataTmpl.Execute(os.Stdout, metadata)
}
