package pattern

import (
	"context"

	"github.com/zhangbiao2009/log_agent/internal/ingest"
)

// PatternEngineConfig controls the PatternEngine. It separates engine-level
// options (JSON extraction) from Drain's pure pattern-matching config.
type PatternEngineConfig struct {
	Drain              DrainConfig
	ExtractJSONMessage bool // extract msg/method/err from JSON before Drain (default: true)
}

// PatternEngine wraps Drain and provides the channel-based pipeline stage.
// It enriches each LogLine with PatternID and PatternTemplate.
type PatternEngine struct {
	drain  *Drain
	config PatternEngineConfig
}

// NewPatternEngine creates a PatternEngine with the given config.
// ExtractJSONMessage defaults to true if the zero config is passed.
func NewPatternEngine(cfg PatternEngineConfig) *PatternEngine {
	// Default ExtractJSONMessage to true when not set explicitly.
	// Callers who want false must set it explicitly in their config.
	return &PatternEngine{
		drain:  NewDrain(cfg.Drain),
		config: cfg,
	}
}

// Run consumes filtered log lines and emits them with PatternID and
// PatternTemplate set. All other fields are passed through unchanged,
// including Raw (the original log text).
// The output channel is closed when ctx is cancelled or in closes.
func (e *PatternEngine) Run(ctx context.Context, in <-chan ingest.LogLine) <-chan ingest.LogLine {
	out := make(chan ingest.LogLine, cap(in))
	go func() {
		defer close(out)
		for {
			select {
			case <-ctx.Done():
				return
			case line, ok := <-in:
				if !ok {
					return
				}
				line = e.enrich(line)
				select {
				case out <- line:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}

// enrich stamps PatternID and PatternTemplate onto the line.
func (e *PatternEngine) enrich(line ingest.LogLine) ingest.LogLine {
	var preprocessed string
	if e.config.ExtractJSONMessage {
		preprocessed = Preprocess(line.Raw)
	} else {
		// Skip JSON extraction; still apply regex normalization.
		preprocessed = applyRegexNorm(line.Raw)
	}
	pat := e.drain.Process(preprocessed)
	line.PatternID = pat.ID
	line.PatternTemplate = pat.Template
	return line
}

// applyRegexNorm applies only the regex normalizations (no JSON extraction).
// Used when ExtractJSONMessage is false.
func applyRegexNorm(raw string) string {
	s := uuidRe.ReplaceAllString(raw, "<UUID>")
	s = ipv4Re.ReplaceAllString(s, "<IP>")
	s = hexRe.ReplaceAllString(s, "<HEX>")
	s = numRe.ReplaceAllString(s, "<NUM>")
	return s
}
