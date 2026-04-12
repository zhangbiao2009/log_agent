package ingest

import (
	"context"
	"time"
)

// LogLine is a single log entry with metadata.
type LogLine struct {
	Service         string
	Timestamp       time.Time
	Level           string // ERROR, FATAL, WARN, or "" if unknown
	Raw             string // original log text
	PatternID       string // set by PatternEngine (empty if pattern detection disabled)
	PatternTemplate string // human-readable template (set alongside PatternID)
}

// LogSource streams log lines from an external system.
type LogSource interface {
	Stream(ctx context.Context) (<-chan LogLine, error)
}
