package raft

import (
	"log/slog"
	"sync/atomic"
)

// CircuitBreakerStore wraps a Store with structured error logging and a
// consecutive-write-failure counter. It implements the Store interface so
// callers don't change at all — they just call methods on Store as usual.
//
// Counting policy:
//   - Writes (SetCurrentTerm, SetVotedFor, PatchEntries, TruncateFrom):
//     errors increment the counter; successes reset it. When the counter
//     reaches Config.StoreErrorThreshold the breaker trips (one-shot until Reset).
//   - Reads: errors are logged but don't touch the counter. Reads have
//     fallback paths in callers (reject the RPC), so a single read failure
//     shouldn't kill the node.
//
// LogIndexOutOfBoundsError is a domain sentinel — never logged, never counted.
//
// Tripped() is queried by node.go between state transitions; states themselves
// don't need to know about the breaker.
type CircuitBreakerStore struct {
	inner     Store
	threshold uint32
	logger    *slog.Logger

	consecutiveWriteErrs atomic.Uint32
	tripped              atomic.Bool
}

// NewCircuitBreakerStore wraps inner. A threshold of 0 means "never trip"
// (caller could pass through unwrapped instead; see RaftNode.Boot).
func NewCircuitBreakerStore(inner Store, threshold uint32, logger *slog.Logger) *CircuitBreakerStore {
	return &CircuitBreakerStore{
		inner:     inner,
		threshold: threshold,
		logger:    logger.With("component", "circuit_breaker_store"),
	}
}

// Tripped reports whether the breaker has fired. Sticky until Reset().
func (c *CircuitBreakerStore) Tripped() bool {
	return c.tripped.Load()
}

// Reset clears the consecutive-error counter and untrips the breaker.
// Called by RaftNode.Boot so every (re)boot starts fresh. Not safe to call
// while state goroutines are actively using the Store.
func (c *CircuitBreakerStore) Reset() {
	c.consecutiveWriteErrs.Store(0)
	c.tripped.Store(false)
}

// recordWrite accounts for a write op's result and logs failures.
func (c *CircuitBreakerStore) recordWrite(op string, err error) error {
	if err == nil {
		c.consecutiveWriteErrs.Store(0)
		return nil
	}
	count := c.consecutiveWriteErrs.Add(1)
	c.logger.Error("store write failed", "op", op, "error", err, "consecutive", count)
	if c.threshold > 0 && count >= c.threshold {
		if c.tripped.CompareAndSwap(false, true) {
			c.logger.Error("store circuit breaker tripped", "threshold", c.threshold)
		}
	}
	return err
}

// recordRead logs read failures but doesn't move the counter or trip the breaker.
// LogIndexOutOfBoundsError is a domain sentinel and is not logged.
func (c *CircuitBreakerStore) recordRead(op string, err error) error {
	if err == nil {
		return nil
	}
	if _, ok := err.(LogIndexOutOfBoundsError); ok {
		return err
	}
	c.logger.Error("store read failed", "op", op, "error", err)
	return err
}

// ─── writes (counted) ────────────────────────────────────────────────

func (c *CircuitBreakerStore) SetCurrentTerm(term Term) error {
	return c.recordWrite("SetCurrentTerm", c.inner.SetCurrentTerm(term))
}

func (c *CircuitBreakerStore) SetVotedFor(nodeId NodeId) error {
	return c.recordWrite("SetVotedFor", c.inner.SetVotedFor(nodeId))
}

func (c *CircuitBreakerStore) PatchEntries(entries []LogEntry) error {
	return c.recordWrite("PatchEntries", c.inner.PatchEntries(entries))
}

func (c *CircuitBreakerStore) TruncateFrom(index LogIndex) error {
	return c.recordWrite("TruncateFrom", c.inner.TruncateFrom(index))
}

// ─── reads (logged, not counted) ─────────────────────────────────────

func (c *CircuitBreakerStore) GetLogEntry(index LogIndex) (*LogEntry, error) {
	entry, err := c.inner.GetLogEntry(index)
	return entry, c.recordRead("GetLogEntry", err)
}

func (c *CircuitBreakerStore) GetLogTerm(index LogIndex) (Term, error) {
	term, err := c.inner.GetLogTerm(index)
	return term, c.recordRead("GetLogTerm", err)
}

func (c *CircuitBreakerStore) GetLogEntries(startIndex LogIndex, endIndex LogIndex) ([]LogEntry, error) {
	entries, err := c.inner.GetLogEntries(startIndex, endIndex)
	return entries, c.recordRead("GetLogEntries", err)
}

func (c *CircuitBreakerStore) GetLastLogIndex() (LogIndex, error) {
	idx, err := c.inner.GetLastLogIndex()
	return idx, c.recordRead("GetLastLogIndex", err)
}

func (c *CircuitBreakerStore) GetLastLogTerm() (Term, error) {
	term, err := c.inner.GetLastLogTerm()
	return term, c.recordRead("GetLastLogTerm", err)
}

func (c *CircuitBreakerStore) GetFirstLogIndex(term Term) (LogIndex, error) {
	idx, err := c.inner.GetFirstLogIndex(term)
	return idx, c.recordRead("GetFirstLogIndex", err)
}

// ─── pure passthroughs (no error to log) ─────────────────────────────

func (c *CircuitBreakerStore) GetCurrentTerm() Term {
	return c.inner.GetCurrentTerm()
}

func (c *CircuitBreakerStore) GetVotedFor() NodeId {
	return c.inner.GetVotedFor()
}
