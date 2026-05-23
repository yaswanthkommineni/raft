package raft

import (
	"errors"
	"log/slog"
	"time"
)

// Config holds all static configuration for a Raft node.
// Section 1: algorithm tuning. Section 2: node identity and wiring.
//
// Today these are hardcoded via DefaultConfig(). Later, a loader will
// populate the same struct from a file or env vars.
type Config struct {
	// ─── Algorithm tuning ────────────────────────────────────────────
	// These control Raft's behavior. Same across all nodes in a cluster.
	ElectionTimeoutMin   time.Duration // lower bound of randomized election timeout
	ElectionTimeoutMax   time.Duration // upper bound of randomized election timeout
	HeartbeatInterval    time.Duration // how often leader sends AppendEntries; must be << ElectionTimeoutMin
	MaxEntriesPerAppend  int           // batch size cap for AppendEntries
	RequestChannelBuffer int           // buffer size for AppendEntriesCh / RequestVoteCh / ClientRequestCh
	MaxNodes             int
	// ─── Node identity and wiring ────────────────────────────────────
	// These are unique per node.
	NodeId        NodeId // this node's identity
	ListenAddr    string // address this node's gRPC server binds to
	StoragePath   string // where the disk LogStore writes (empty = in-memory only)
	ReferenceNode Node   // empty = bootstrap mode; non-empty = join an existing cluster via this node
	LogLevel      string

	// Logger is the structured logger used by the raft package. If nil, slog.Default() is used.
	// Callers control destination/format by wiring the slog.Handler of their choice.
	Logger *slog.Logger

	// StoreErrorThreshold is the maximum number of consecutive Store *write* failures
	// tolerated before the breaker trips and the node escalates to AbortState.
	// Read failures are logged by the wrapper but never trip the breaker. A successful
	// write resets the counter. Set to 0 to disable the breaker (Store passes through unwrapped).
	StoreErrorThreshold uint32
}

func DefaultConfig() Config {
	return Config{
		ElectionTimeoutMin:   150 * time.Millisecond,
		ElectionTimeoutMax:   300 * time.Millisecond,
		HeartbeatInterval:    50 * time.Millisecond,
		MaxEntriesPerAppend:  64,
		RequestChannelBuffer: 16,
		MaxNodes:             10,
		NodeId:               1,
		ListenAddr:           "127.0.0.1:9000",
		StoragePath:          "",     // in-memory for now
		ReferenceNode:        Node{}, // bootstrap mode
		LogLevel:             "info",
		Logger:               slog.Default(),
		StoreErrorThreshold:  10,
	}
}

func (c *Config) Validate() error {
	// Heartbeat must be well below election timeout, otherwise leaders
	// get falsely deposed by their own followers.
	// Rule of thumb from the paper: heartbeat ≤ electionTimeoutMin / 3
	// ...
	if c.MaxNodes < 1 || c.MaxNodes > 20 {
		return errors.New("MaxNodes must be between 1 and 20")
	}
	return nil
}
