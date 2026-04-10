package testutil

import (
	"time"
)

// MockAlert mirrors notify.Alert to avoid import cycles.
type MockAlert struct {
	Service     string
	Level       string
	Count       int
	Window      time.Duration
	SampleLines []string
	Timestamp   time.Time
}
