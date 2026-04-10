package testutil

import (
	"sync"
	"time"
)

type FakeClock struct {
	mu      sync.Mutex
	now     time.Time
	waiters []waiter
}

type waiter struct {
	deadline time.Time
	ch       chan time.Time
}

func NewFakeClock(now time.Time) *FakeClock {
	return &FakeClock{now: now}
}

func (c *FakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *FakeClock) After(d time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	ch := make(chan time.Time, 1)
	deadline := c.now.Add(d)
	if !c.now.Before(deadline) {
		ch <- c.now
		return ch
	}
	c.waiters = append(c.waiters, waiter{deadline: deadline, ch: ch})
	return ch
}

func (c *FakeClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var remaining []waiter
	for _, w := range c.waiters {
		if !now.Before(w.deadline) {
			w.ch <- now
		} else {
			remaining = append(remaining, w)
		}
	}
	c.waiters = remaining
	c.mu.Unlock()
}
