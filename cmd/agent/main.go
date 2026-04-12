package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/anomaly"
	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/correlator"
	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/ingest"
	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/notify"
	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/pattern"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Loki         LokiConfig         `yaml:"loki"`
	Source       SourceConfig       `yaml:"source"`
	Aggregation  AggregationConfig  `yaml:"aggregation"`
	Notification NotificationConfig `yaml:"notification"`
	Pattern      PatternConfig      `yaml:"pattern"`
	Anomaly      AnomalyConfig      `yaml:"anomaly"`
	Correlator   CorrelatorConfig   `yaml:"correlator"`
}

// SourceConfig selects which log source to use.
// type: "loki" (default) or "file" (for local testing without Loki).
type SourceConfig struct {
	Type string     `yaml:"type"` // "loki" | "file"
	File FileSource `yaml:"file"`
}

type FileSource struct {
	Path string `yaml:"path"`
}

type LokiConfig struct {
	URL               string `yaml:"url"`
	Query             string `yaml:"query"`
	PollInterval      string `yaml:"poll_interval"`
	TenantID          string `yaml:"tenant_id"`
	ServiceLabel      string `yaml:"service_label"`
	BasicAuthUser     string `yaml:"basic_auth_user"`
	BasicAuthPassword string `yaml:"basic_auth_password"`
}

type AggregationConfig struct {
	Window   string `yaml:"window"`
	MinCount int    `yaml:"min_count"`
}

type NotificationConfig struct {
	Channels []ChannelConfig `yaml:"channels"`
}

type PatternConfig struct {
	Enabled            bool    `yaml:"enabled"`
	Depth              int     `yaml:"depth"`
	Similarity         float64 `yaml:"similarity"`
	MaxChildren        int     `yaml:"max_children"`
	MaxPatterns        int     `yaml:"max_patterns"`
	ExtractJSONMessage bool    `yaml:"extract_json_message"`
}

type AnomalyConfig struct {
	Enabled         bool    `yaml:"enabled"`
	SpikeMultiplier float64 `yaml:"spike_multiplier"`
	RateJumpFactor  float64 `yaml:"rate_jump_factor"`
	EMAAlpha        float64 `yaml:"ema_alpha"`
	MinSamples      int     `yaml:"min_samples"`
	NewPatternGrace string  `yaml:"new_pattern_grace"`
}

type ChannelConfig struct {
	Type       string `yaml:"type"`
	WebhookURL string `yaml:"webhook_url"`
}

type CorrelatorConfig struct {
	Enabled          bool   `yaml:"enabled"`
	Window           string `yaml:"window"`
	DependenciesFile string `yaml:"dependencies_file"`
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}

	// Expand environment variables.
	expanded := os.ExpandEnv(string(data))

	var cfg Config
	if err := yaml.Unmarshal([]byte(expanded), &cfg); err != nil {
		return nil, fmt.Errorf("parse config: %w", err)
	}

	return &cfg, nil
}

func parseDuration(s string, defaultVal time.Duration) time.Duration {
	if s == "" {
		return defaultVal
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		slog.Warn("invalid duration, using default", "value", s, "default", defaultVal)
		return defaultVal
	}
	return d
}

func buildNotifiers(cfg NotificationConfig) []notify.Notifier {
	var notifiers []notify.Notifier
	for _, ch := range cfg.Channels {
		switch ch.Type {
		case "slack":
			if ch.WebhookURL == "" {
				slog.Warn("slack notifier configured but webhook_url is empty, skipping")
				continue
			}
			notifiers = append(notifiers, notify.NewSlackNotifier(ch.WebhookURL))
			slog.Info("registered notifier", "type", "slack")
		case "log":
			notifiers = append(notifiers, notify.NewLogNotifier(nil))
			slog.Info("registered notifier", "type", "log")
		default:
			slog.Warn("unknown notifier type, skipping", "type", ch.Type)
		}
	}
	return notifiers
}

func buildSource(cfg *Config) (ingest.LogSource, error) {
	sourceType := cfg.Source.Type
	if sourceType == "" {
		sourceType = "loki" // default for backward compatibility
	}
	switch sourceType {
	case "loki":
		return ingest.NewLokiSource(ingest.LokiConfig{
			URL:               cfg.Loki.URL,
			Query:             cfg.Loki.Query,
			PollInterval:      parseDuration(cfg.Loki.PollInterval, 10*time.Second),
			TenantID:          cfg.Loki.TenantID,
			ServiceLabel:      cfg.Loki.ServiceLabel,
			BasicAuthUser:     cfg.Loki.BasicAuthUser,
			BasicAuthPassword: cfg.Loki.BasicAuthPassword,
		}), nil
	case "file":
		path := cfg.Source.File.Path
		if path == "" {
			return nil, fmt.Errorf("source.file.path must be set when source.type is \"file\"")
		}
		slog.Info("using file source", "path", path)
		return ingest.NewFileSource(ingest.FileConfig{Path: path}), nil
	default:
		return nil, fmt.Errorf("unknown source type %q (must be \"loki\" or \"file\")", sourceType)
	}
}

func run() error {
	configPath := "config/config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	window := parseDuration(cfg.Aggregation.Window, 1*time.Minute)
	minCount := cfg.Aggregation.MinCount
	if minCount == 0 {
		minCount = 1
	}

	// Build pipeline components.
	source, err := buildSource(cfg)
	if err != nil {
		return fmt.Errorf("build source: %w", err)
	}

	notifiers := buildNotifiers(cfg.Notification)
	if len(notifiers) == 0 {
		slog.Warn("no notifiers configured, adding log notifier as fallback")
		notifiers = append(notifiers, notify.NewLogNotifier(nil))
	}
	dispatcher := notify.NewDispatcher(notifiers...)

	aggregator := notify.NewAggregator(window, minCount)

	// Wire pipeline with graceful shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	slog.Info("starting log agent",
		"source", cfg.Source.Type,
		"window", window,
		"min_count", minCount,
	)

	logCh, err := source.Stream(ctx)
	if err != nil {
		return fmt.Errorf("start log source: %w", err)
	}

	filtered := ingest.Filter(ctx, logCh)

	var enriched <-chan ingest.LogLine
	if cfg.Pattern.Enabled {
		pe := pattern.NewPatternEngine(pattern.PatternEngineConfig{
			Drain: pattern.DrainConfig{
				Depth:               cfg.Pattern.Depth,
				SimilarityThreshold: cfg.Pattern.Similarity,
				MaxChildren:         cfg.Pattern.MaxChildren,
				MaxPatterns:         cfg.Pattern.MaxPatterns,
			},
			ExtractJSONMessage: cfg.Pattern.ExtractJSONMessage,
		})
		enriched = pe.Run(ctx, filtered)
		slog.Info("pattern engine enabled",
			"depth", cfg.Pattern.Depth,
			"similarity", cfg.Pattern.Similarity,
		)
	} else {
		enriched = filtered
	}

	alerts := aggregator.Run(ctx, enriched)

	var anomalous <-chan notify.Alert
	if cfg.Anomaly.Enabled {
		detector := anomaly.NewAnomalyDetector(anomaly.AnomalyConfig{
			SpikeMultiplier: cfg.Anomaly.SpikeMultiplier,
			RateJumpFactor:  cfg.Anomaly.RateJumpFactor,
			EMAAlpha:        cfg.Anomaly.EMAAlpha,
			MinSamples:      cfg.Anomaly.MinSamples,
			NewPatternGrace: parseDuration(cfg.Anomaly.NewPatternGrace, 24*time.Hour),
		}, anomaly.NewMemoryStore())
		anomalous = detector.Run(ctx, alerts)
		slog.Info("anomaly detector enabled",
			"spike_multiplier", cfg.Anomaly.SpikeMultiplier,
			"rate_jump_factor", cfg.Anomaly.RateJumpFactor,
			"ema_alpha", cfg.Anomaly.EMAAlpha,
			"min_samples", cfg.Anomaly.MinSamples,
			"new_pattern_grace", cfg.Anomaly.NewPatternGrace,
		)
	} else {
		anomalous = alerts
	}

	// Stage 5: Correlator (or bypass).
	var incidents <-chan notify.Incident
	if cfg.Correlator.Enabled {
		graph, err := correlator.LoadFromYAML(cfg.Correlator.DependenciesFile)
		if err != nil {
			return fmt.Errorf("load dependencies: %w", err)
		}
		correlatorWindow := parseDuration(cfg.Correlator.Window, 2*time.Minute)
		c := correlator.NewCorrelator(correlator.CorrelatorConfig{
			Window: correlatorWindow,
		}, graph)
		incidents = c.Run(ctx, anomalous)
		slog.Info("correlator enabled",
			"window", correlatorWindow,
			"dependencies_file", cfg.Correlator.DependenciesFile,
		)
	} else {
		incidents = correlator.WrapAlerts(ctx, anomalous)
	}

	for inc := range incidents {
		if err := dispatcher.Dispatch(ctx, inc); err != nil {
			slog.Error("dispatch failed", "err", err)
		}
	}

	slog.Info("log agent stopped")
	return nil
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}
