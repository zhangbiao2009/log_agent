package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/ingest"
	"github.com/zhangbiao2009/agent_exercise/log_agent/internal/notify"
	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration.
type Config struct {
	Loki         LokiConfig         `yaml:"loki"`
	Aggregation  AggregationConfig  `yaml:"aggregation"`
	Notification NotificationConfig `yaml:"notification"`
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

type ChannelConfig struct {
	Type       string `yaml:"type"`
	WebhookURL string `yaml:"webhook_url"`
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

func run() error {
	configPath := "config/config.yaml"
	if len(os.Args) > 1 {
		configPath = os.Args[1]
	}

	cfg, err := loadConfig(configPath)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	pollInterval := parseDuration(cfg.Loki.PollInterval, 10*time.Second)
	window := parseDuration(cfg.Aggregation.Window, 1*time.Minute)
	minCount := cfg.Aggregation.MinCount
	if minCount == 0 {
		minCount = 1
	}

	// Build pipeline components.
	source := ingest.NewLokiSource(ingest.LokiConfig{
		URL:               cfg.Loki.URL,
		Query:             cfg.Loki.Query,
		PollInterval:      pollInterval,
		TenantID:          cfg.Loki.TenantID,
		ServiceLabel:      cfg.Loki.ServiceLabel,
		BasicAuthUser:     cfg.Loki.BasicAuthUser,
		BasicAuthPassword: cfg.Loki.BasicAuthPassword,
	})

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
		"loki_url", cfg.Loki.URL,
		"poll_interval", pollInterval,
		"window", window,
		"min_count", minCount,
	)

	logCh, err := source.Stream(ctx)
	if err != nil {
		return fmt.Errorf("start log source: %w", err)
	}

	filtered := ingest.Filter(ctx, logCh)
	alerts := aggregator.Run(ctx, filtered)

	for alert := range alerts {
		if err := dispatcher.Dispatch(ctx, alert); err != nil {
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
