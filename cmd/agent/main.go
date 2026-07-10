package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zhangbiao2009/log_agent/internal/anomaly"
	"github.com/zhangbiao2009/log_agent/internal/correlator"
	"github.com/zhangbiao2009/log_agent/internal/diagnosis"
	"github.com/zhangbiao2009/log_agent/internal/ingest"
	"github.com/zhangbiao2009/log_agent/internal/notify"
	"github.com/zhangbiao2009/log_agent/internal/pattern"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Source       SourceConfig       `yaml:"source"`
	Services     []ServiceConfig    `yaml:"services"`
	Aggregation  AggregationConfig  `yaml:"aggregation"`
	Notification NotificationConfig `yaml:"notification"`
	Pattern      PatternConfig      `yaml:"pattern"`
	Anomaly      AnomalyConfig      `yaml:"anomaly"`
	Correlator   CorrelatorConfig   `yaml:"correlator"`
	Diagnosis    DiagnosisConfig    `yaml:"diagnosis"`
}

// SourceConfig selects the source type and shared connection settings.
// Each entry in the top-level Services list becomes an independent pipeline.
type SourceConfig struct {
	Type string   `yaml:"type"` // "loki" | "file"
	Loki LokiConn `yaml:"loki"` // shared Loki connection (type == "loki")
}

// LokiConn holds the Loki connection settings shared by every service. The
// per-service LogQL selector lives in ServiceConfig.Query.
type LokiConn struct {
	URL               string `yaml:"url"`
	PollInterval      string `yaml:"poll_interval"`
	TenantID          string `yaml:"tenant_id"`
	ServiceLabel      string `yaml:"service_label"`
	BasicAuthUser     string `yaml:"basic_auth_user"`
	BasicAuthPassword string `yaml:"basic_auth_password"`
}

// ServiceConfig defines one monitored service. It gets its own end-to-end
// pipeline (source → filter → pattern → aggregate → anomaly). Depending on
// Source.Type, either Query (loki) or Path (file) is used.
type ServiceConfig struct {
	Name  string `yaml:"name"`
	Query string `yaml:"query"` // LogQL selector, used when source.type == "loki"
	Path  string `yaml:"path"`  // NDJSON file, used when source.type == "file"
}

type AggregationConfig struct {
	Window   string `yaml:"window"`
	MinCount int    `yaml:"min_count"`
}

type NotificationConfig struct {
	DedupWindow   string          `yaml:"dedup_window"`
	ResolveAfter  string          `yaml:"resolve_after"`
	CheckInterval string          `yaml:"check_interval"`
	Channels      []ChannelConfig `yaml:"channels"`
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
	Type         string   `yaml:"type"`
	Severities   []string `yaml:"severities"`
	WebhookURL   string   `yaml:"webhook_url"`
	SMTPHost     string   `yaml:"smtp_host"`
	SMTPPort     int      `yaml:"smtp_port"`
	SMTPUsername string   `yaml:"smtp_username"`
	SMTPPassword string   `yaml:"smtp_password"`
	From         string   `yaml:"from"`
	Recipients   []string `yaml:"recipients"`
	UseTLS       *bool    `yaml:"use_tls"`
}

type CorrelatorConfig struct {
	Enabled          bool   `yaml:"enabled"`
	Window           string `yaml:"window"`
	DependenciesFile string `yaml:"dependencies_file"`
}

type DiagnosisConfig struct {
	Enabled     bool    `yaml:"enabled"`
	Endpoint    string  `yaml:"endpoint"`
	Model       string  `yaml:"model"`
	MaxTokens   int     `yaml:"max_tokens"`
	Temperature float64 `yaml:"temperature"`
	Timeout     string  `yaml:"timeout"`
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

func buildNotifiers(cfg NotificationConfig) []notify.NotifierRoute {
	var routes []notify.NotifierRoute
	for _, ch := range cfg.Channels {
		var n notify.Notifier
		switch ch.Type {
		case "slack":
			if ch.WebhookURL == "" {
				slog.Warn("slack notifier configured but webhook_url is empty, skipping")
				continue
			}
			n = notify.NewSlackNotifier(ch.WebhookURL)
		case "log":
			n = notify.NewLogNotifier(nil)
		case "email":
			useTLS := true
			if ch.UseTLS != nil {
				useTLS = *ch.UseTLS
			}
			n = notify.NewEmailNotifier(notify.EmailConfig{
				Host:       ch.SMTPHost,
				Port:       ch.SMTPPort,
				Username:   ch.SMTPUsername,
				Password:   ch.SMTPPassword,
				From:       ch.From,
				Recipients: ch.Recipients,
				UseTLS:     useTLS,
			})
		case "teams":
			if ch.WebhookURL == "" {
				slog.Warn("teams notifier configured but webhook_url is empty, skipping")
				continue
			}
			n = notify.NewTeamsNotifier(notify.TeamsConfig{WebhookURL: ch.WebhookURL})
		default:
			slog.Warn("unknown notifier type, skipping", "type", ch.Type)
			continue
		}
		slog.Info("registered notifier", "type", ch.Type, "severities", ch.Severities)
		routes = append(routes, notify.NotifierRoute{
			Notifier:   n,
			Severities: ch.Severities,
		})
	}
	return routes
}

// buildSource creates a LogSource for a single service based on the global
// source type and the service's per-service selector (Query for loki, Path
// for file). The service name is forced onto every emitted line so downstream
// keying is correct regardless of label content.
func buildSource(cfg *Config, svc ServiceConfig) (ingest.LogSource, error) {
	sourceType := cfg.Source.Type
	if sourceType == "" {
		sourceType = "loki"
	}
	switch sourceType {
	case "loki":
		if svc.Query == "" {
			return nil, fmt.Errorf("service %q: query must be set when source.type is \"loki\"", svc.Name)
		}
		return ingest.NewLokiSource(ingest.LokiConfig{
			URL:               cfg.Source.Loki.URL,
			Query:             svc.Query,
			PollInterval:      parseDuration(cfg.Source.Loki.PollInterval, 10*time.Second),
			TenantID:          cfg.Source.Loki.TenantID,
			ServiceLabel:      cfg.Source.Loki.ServiceLabel,
			BasicAuthUser:     cfg.Source.Loki.BasicAuthUser,
			BasicAuthPassword: cfg.Source.Loki.BasicAuthPassword,
			Service:           svc.Name,
		}), nil
	case "file":
		if svc.Path == "" {
			return nil, fmt.Errorf("service %q: path must be set when source.type is \"file\"", svc.Name)
		}
		slog.Info("using file source", "service", svc.Name, "path", svc.Path)
		return ingest.NewFileSource(ingest.FileConfig{Path: svc.Path, Service: svc.Name}), nil
	default:
		return nil, fmt.Errorf("unknown source type %q (must be \"loki\" or \"file\")", sourceType)
	}
}

// buildServicePipeline wires the per-service zone for one service:
// source → filter → pattern → aggregate → anomaly, returning the service's
// Alert channel. All stages run in their own goroutines with state isolated
// to this service (own Drain tree, own aggregation window, own baselines).
func buildServicePipeline(ctx context.Context, cfg *Config, svc ServiceConfig, window time.Duration, minCount int) (<-chan notify.Alert, error) {
	source, err := buildSource(cfg, svc)
	if err != nil {
		return nil, err
	}

	logCh, err := source.Stream(ctx)
	if err != nil {
		return nil, fmt.Errorf("service %q: start source: %w", svc.Name, err)
	}

	filtered := ingest.Filter(ctx, logCh)

	enriched := filtered
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
	}

	alerts := notify.NewAggregator(window, minCount).Run(ctx, enriched)

	anomalous := alerts
	if cfg.Anomaly.Enabled {
		detector := anomaly.NewAnomalyDetector(anomaly.AnomalyConfig{
			SpikeMultiplier: cfg.Anomaly.SpikeMultiplier,
			RateJumpFactor:  cfg.Anomaly.RateJumpFactor,
			EMAAlpha:        cfg.Anomaly.EMAAlpha,
			MinSamples:      cfg.Anomaly.MinSamples,
			NewPatternGrace: parseDuration(cfg.Anomaly.NewPatternGrace, 24*time.Hour),
		}, anomaly.NewMemoryStore())
		anomalous = detector.Run(ctx, alerts)
	}

	return anomalous, nil
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

	if len(cfg.Services) == 0 {
		return fmt.Errorf("no services configured; add at least one entry under \"services\"")
	}

	routes := buildNotifiers(cfg.Notification)
	if len(routes) == 0 {
		slog.Warn("no notifiers configured, adding log notifier as fallback")
		routes = append(routes, notify.NotifierRoute{Notifier: notify.NewLogNotifier(nil)})
	}
	dispatcher := notify.NewRoutedDispatcher(routes)

	// Wire pipeline with graceful shutdown.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	sourceType := cfg.Source.Type
	if sourceType == "" {
		sourceType = "loki"
	}
	slog.Info("starting log agent",
		"source", sourceType,
		"services", len(cfg.Services),
		"window", window,
		"min_count", minCount,
	)

	// Fan-out: one independent pipeline per service (source → filter →
	// pattern → aggregate → anomaly), each producing an Alert channel.
	alertChans := make([]<-chan notify.Alert, 0, len(cfg.Services))
	for _, svc := range cfg.Services {
		if svc.Name == "" {
			return fmt.Errorf("every service entry must have a name")
		}
		ch, err := buildServicePipeline(ctx, cfg, svc, window, minCount)
		if err != nil {
			return fmt.Errorf("build pipeline: %w", err)
		}
		slog.Info("service pipeline started", "service", svc.Name)
		alertChans = append(alertChans, ch)
	}
	if cfg.Pattern.Enabled {
		slog.Info("pattern engine enabled", "depth", cfg.Pattern.Depth, "similarity", cfg.Pattern.Similarity)
	}
	if cfg.Anomaly.Enabled {
		slog.Info("anomaly detector enabled",
			"spike_multiplier", cfg.Anomaly.SpikeMultiplier,
			"rate_jump_factor", cfg.Anomaly.RateJumpFactor,
			"ema_alpha", cfg.Anomaly.EMAAlpha,
			"min_samples", cfg.Anomaly.MinSamples,
		)
	}

	// Fan-in: merge all per-service Alert channels into one before the
	// shared cross-service stages.
	anomalous := notify.MergeAlerts(ctx, alertChans...)

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

	// Stage 6: Diagnoser (or pass-through).
	var diagnosed <-chan notify.Incident
	if cfg.Diagnosis.Enabled {
		apiKey := os.Getenv("LLM_API_KEY")
		if apiKey == "" {
			return fmt.Errorf("LLM_API_KEY must be set when diagnosis is enabled")
		}
		diagCfg := diagnosis.DiagnoserConfig{
			Endpoint:    cfg.Diagnosis.Endpoint,
			Model:       cfg.Diagnosis.Model,
			MaxTokens:   cfg.Diagnosis.MaxTokens,
			Temperature: cfg.Diagnosis.Temperature,
			Timeout:     parseDuration(cfg.Diagnosis.Timeout, 30*time.Second),
		}
		client := diagnosis.NewHTTPClient(diagCfg, apiKey)
		diagnoser := diagnosis.NewDiagnoser(diagCfg, client)
		diagnosed = diagnoser.Run(ctx, incidents)
		slog.Info("diagnoser enabled",
			"model", diagCfg.Model,
			"endpoint", diagCfg.Endpoint,
		)
	} else {
		diagnosed = incidents
	}

	// Stage 7: Lifecycle Manager (dedup + auto-resolve).
	lifecycleCfg := notify.LifecycleConfig{
		DedupWindow:   parseDuration(cfg.Notification.DedupWindow, 5*time.Minute),
		ResolveAfter:  parseDuration(cfg.Notification.ResolveAfter, 10*time.Minute),
		CheckInterval: parseDuration(cfg.Notification.CheckInterval, 1*time.Minute),
	}
	lm := notify.NewLifecycleManager(lifecycleCfg)
	managed := lm.Run(ctx, diagnosed)
	slog.Info("lifecycle manager enabled",
		"dedup_window", lifecycleCfg.DedupWindow,
		"resolve_after", lifecycleCfg.ResolveAfter,
	)

	for inc := range managed {
		if err := dispatcher.Dispatch(ctx, inc); err != nil {
			slog.Error("dispatch failed", "err", err, "event", inc.EventType, "id", inc.ID)
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
