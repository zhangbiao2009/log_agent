package ingest

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"
)

// FileConfig configures a file-based log source.
type FileConfig struct {
	// Path is the NDJSON file to read. Each line must be a JSON object with
	// fields: service (string), timestamp (RFC3339, optional), raw (string).
	// level is optional; if absent the Filter stage infers it from raw text.
	Path string

	// Service, when non-empty, overrides the per-line "service" field in the
	// NDJSON file. Used in the per-service pipeline model where each file
	// belongs to exactly one service, so the file lines can omit "service".
	Service string
}

// fileRecord is the JSON shape of each line in the NDJSON fixture file.
// Two mutually exclusive shapes are supported:
//
//  1. Log line:   {"service":"...","timestamp":"...","raw":"..."}
//  2. Pause sentinel: {"pause":"5s"}
//     The FileSource sleeps for the given duration, allowing the Aggregator's
//     real-clock window timer to fire and flush the current window before
//     the next batch of log lines arrives.
type fileRecord struct {
	// Log line fields
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"` // RFC3339; defaults to now if blank
	Level     string `json:"level"`     // optional; Filter will infer if blank
	Raw       string `json:"raw"`

	// Sentinel field — mutually exclusive with the log line fields above.
	Pause string `json:"pause"` // Go duration string, e.g. "5s"
}

// FileSource implements LogSource by replaying an NDJSON file.
// When all lines have been emitted the output channel is closed, which
// causes the downstream pipeline to drain and the agent to exit cleanly.
type FileSource struct {
	cfg FileConfig
}

func NewFileSource(cfg FileConfig) *FileSource {
	return &FileSource{cfg: cfg}
}

func (s *FileSource) Stream(ctx context.Context) (<-chan LogLine, error) {
	f, err := os.Open(s.cfg.Path)
	if err != nil {
		return nil, fmt.Errorf("open log fixture: %w", err)
	}

	out := make(chan LogLine, 1000)
	go func() {
		defer close(out)
		defer f.Close()

		scanner := bufio.NewScanner(f)
		// Allow lines up to 1 MiB (some log lines can be large JSON blobs).
		scanner.Buffer(make([]byte, 1<<20), 1<<20)

		lineNum := 0
		for scanner.Scan() {
			lineNum++
			line := scanner.Text()
			if line == "" || line[0] == '#' {
				continue // skip blank lines and comments
			}

			var rec fileRecord
			if err := json.Unmarshal([]byte(line), &rec); err != nil {
				slog.Warn("file source: skipping malformed line",
					"line_num", lineNum, "err", err)
				continue
			}

			// Handle pause sentinel: sleep to let the Aggregator window timer fire.
			if rec.Pause != "" {
				d, err := time.ParseDuration(rec.Pause)
				if err != nil {
					slog.Warn("file source: invalid pause duration, skipping",
						"line_num", lineNum, "pause", rec.Pause)
					continue
				}
				slog.Info("file source: pausing between windows", "duration", d)
				select {
				case <-time.After(d):
				case <-ctx.Done():
					return
				}
				continue
			}

			ts := time.Now()
			if rec.Timestamp != "" {
				if parsed, err := time.Parse(time.RFC3339, rec.Timestamp); err == nil {
					ts = parsed
				}
			}

			service := rec.Service
			if s.cfg.Service != "" {
				service = s.cfg.Service
			}

			out <- LogLine{
				Service:   service,
				Timestamp: ts,
				Level:     rec.Level,
				Raw:       rec.Raw,
			}

			select {
			case <-ctx.Done():
				return
			default:
			}
		}

		if err := scanner.Err(); err != nil {
			slog.Error("file source: scanner error", "err", err)
		}
		slog.Info("file source: finished replaying log file",
			"path", s.cfg.Path, "lines_read", lineNum)
	}()

	return out, nil
}
