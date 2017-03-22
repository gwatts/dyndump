package cmd

import "time"

const (
	maxParallel    = 1000
	statsFrequency = 2 * time.Second
	logFrequency   = 30 * time.Second
	awsMaxRetries  = 10
)
