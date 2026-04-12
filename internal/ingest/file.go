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
}

// fileRecord is the JSON shape of each line in the NDJSON fixture file.
type fileRecord struct {
	Service   string `json:"service"`
	Timestamp string `json:"timestamp"` // RFC3339; defaults to now if blank
	Level     string `json:"level"`     // optional; Filter will infer if blank
	Raw       string `json:"raw"`
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

			ts := time.Now()
			if rec.Timestamp != "" {
				if parsed, err := time.Parse(time.RFC3339, rec.Timestamp); err == nil {
					ts = parsed
				}
			}

			out <- LogLine{
				Service:   rec.Service,
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
