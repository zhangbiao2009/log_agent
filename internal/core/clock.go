// Package core holds the shared domain types that flow through the pipeline
// (Alert, Incident) plus the Clock abstraction used for time injection in
// tests. It has no internal dependencies so every stage can import it without
// creating a cycle.
package core

import "time"

// Clock abstracts time so time-dependent stages can be tested with a fake.
type Clock interface {
	Now() time.Time
	After(d time.Duration) <-chan time.Time
}

type realClock struct{}

func (realClock) Now() time.Time                         { return time.Now() }
func (realClock) After(d time.Duration) <-chan time.Time { return time.After(d) }

// RealClock returns a Clock backed by the standard library time functions.
func RealClock() Clock { return realClock{} }
