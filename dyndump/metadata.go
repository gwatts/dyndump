// Copyright 2016 Gareth Watts
// Licensed under an MIT license
// See the LICENSE file for details

package dyndump

import "time"

// MetadataStatus represents the state of the backup.
type MetadataStatus string

const (
	// StatusRunning represents a backup in progress.
	StatusRunning MetadataStatus = "running"

	// StatusFailed represents an aborted or failed backup.
	StatusFailed MetadataStatus = "failed"

	// StatusCompleted represents a successfully completed backup.
	StatusCompleted MetadataStatus = "completed"
)

// MetadataBackupType represents the type or mode of backup.
type MetadataBackupType string

const (
	// BackupFull is set on a complete backup of a DynamoDB table.
	BackupFull MetadataBackupType = "full"

	// BackupQuery is set on a selective backup of a DynamoDB table.
	BackupQuery MetadataBackupType = "query"
)

// Metadata is stored alongside backups pushed to S3.
type Metadata struct {
	TableName         string             `json:"table_name"`
	TableARN          string             `json:"table_arn"`
	Status            MetadataStatus     `json:"status"`             // "running", "failed" or "completed"
	Type              MetadataBackupType `json:"backup_type"`        // "full" or "query"
	StartTime         time.Time          `json:"backup_start_time"`  // The time the backup started.
	EndTime           *time.Time         `json:"backup_end_time"`    // The time the backup was completed, or failed.
	UncompressedBytes int64              `json:"uncompressed_bytes"` // Size of the uncompressed JSON, in bytes.
	CompressedBytes   int64              `json:"compressed_bytes"`   // Size of the gzipped JSON takes, in bytes.
	ItemCount         int64              `json:"item_count"`         // Number of items in the backup.
	PartCount         int64              `json:"part_count"`         // Number of S3 objects comprising the backup
}
