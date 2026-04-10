package ingest

import (
	"context"
	"time"
)

// LogLine is a single log entry with metadata.
type LogLine struct {
	Service   string
	Timestamp time.Time
	Level     string // ERROR, FATAL, WARN, or "" if unknown
	Raw       string // original log text
}

// LogSource streams log lines from an external system.
type LogSource interface {
	Stream(ctx context.Context) (<-chan LogLine, error)
}
