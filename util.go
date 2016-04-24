package main

import "fmt"

const (
	kib = 1 << 10
	mib = 1 << 20
	gib = 1 << 30
	tib = 1 << 40
)

func fmtBytes(bytes int64) string {
	switch {
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
