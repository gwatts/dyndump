// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package cmd

import (
	"html/template"
	"os"

	"github.com/gwatts/dyndump/dyndump"
	"github.com/gwatts/flagvals"
	cli "github.com/gwatts/mow.cli"
)

func RegisterInfoCommand(app *cli.Cli) {
	app.Command("info", "Display backup metadata from an S3 backup", func(cmd *cli.Cmd) {
		cmd.Spec = "--s3-bucket --s3-prefix"
		action := &metadataDumper{
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

			maxRetries: flagvals.GTEInt(awsMaxRetries, 0),
		}

		cmd.Var(cli.VarOpt{
			Name:   "max-retries",
			Value:  action.maxRetries,
			Desc:   "Maximum number of retry attempts to make with AWS services before failing",
			EnvVar: "AWS_MAX_RETRIES",
		})

		cmd.Action = action.run
	})
}

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
	maxRetries   *flagvals.RangeInt
}

func (md *metadataDumper) run() {
	aws := initAWS(md.maxRetries)
	sr := &dyndump.S3Reader{
		S3:         aws.s3,
		Bucket:     *md.s3BucketName,
		PathPrefix: *md.s3Prefix,
	}
	metadata, err := sr.Metadata()
	if err != nil {
		fail("Failed to read metadata from S3: %v", err)
	}
	metadataTmpl.Execute(os.Stdout, metadata)
}
