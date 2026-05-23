package raft

import (
	"context"
)

// This file contains the interfaces that the Raft implementation will use.

// Not an external system inteface, but an internal one to abstract the Raft node's state and behavior. This allows for better modularity and separation of concerns in the implementation.
// The Store interface defines the methods that a storage backend must implement to be used by the Raft node. This allows for flexibility in choosing different storage implementations, such as in-memory or disk-based storage.
// The store is dumb the ones who are using this should deal with the logic
//
// Each individual operation must be atomic — either succeeds or fails, never
// leaves the store in an inconsistent state.
//
// Store should have a dummy log entry at index 0 with term 0 and log index 0
type Store interface {
	// State persistence - term and votedFor

	// GetCurrentTerm should be served from memory and not the persistent storage, but it should be updated with the persistent storage
	GetCurrentTerm() Term
	// GetVotedFor should be served from memory and not the persistent storage, but it should be updated with the persistent storage
	GetVotedFor() NodeId
	// internally should use SetState
	SetCurrentTerm(term Term) error
	// internally should use SetState
	SetVotedFor(nodeId NodeId) error

	// Local versions of term and votedFor are stored, update them once the SetState is successfull, then only return from the function
	// Encode term and NodeId into a single value and store it into the files
	SetState(term Term, nodeId NodeId) error

	// Could also get an empty slice, if so don't return error
	// just blindly overwrite or append the entries at their index, the ones who are using this should deal with the logic
	// multiple calls can be made to this function but on different indexes, so this should be thread safe
	PatchEntries(entries []LogEntry) error
	// return LogIndexOutOfBoundsError error
	GetLogEntry(index LogIndex) (*LogEntry, error)
	GetLogTerm(index LogIndex) (Term, error)
	GetLogEntries(startIndex LogIndex, endIndex LogIndex) ([]LogEntry, error)
	GetLastLogIndex() (LogIndex, error)
	GetLastLogTerm() (Term, error)
	GetFirstLogIndex(term Term) (LogIndex, error)
	// truncate last ones first
	TruncateFrom(index LogIndex) error
}

// The StateMachine interface defines the methods that a state machine must implement to be used by the Raft node. This allows for flexibility in choosing different state machine implementations, such as a key-value store or a more complex application-specific state machine.
type StateMachine interface {
	Apply(entry *LogEntry) (StateMachineResponseData, error)
}

// Transport abstracts the network layer. The algorithm only knows NodeIds;
// each Transport implementation maintains its own address book (NodeId → address).
// All methods must be safe for concurrent use.
//
// Retry policy: implementations MUST retry transient failures (timeouts,
// connection resets, peer unavailable, etc.) internally with their own
// backoff strategy until the supplied context is canceled or the call
// succeeds. Callers will not retry on their own — they treat a returned
// error as terminal for this attempt. When ctx is canceled, implementations
// should return promptly with ctx.Err() (context.Canceled or
// context.DeadlineExceeded). Non-retryable errors (malformed request,
// permanent peer rejection) should be returned without retry.
type Transport interface {
	// SendAppendEntries sends an AppendEntries RPC to the given node.
	// The context allows the caller to cancel in-flight RPCs (e.g., on shutdown or state transition).
	SendAppendEntries(ctx context.Context, nodeAddress NodeAddress, req AppendEntriesRequest) (AppendEntriesResponse, error)

	// SendRequestVote sends a RequestVote RPC to the given node.
	SendRequestVote(ctx context.Context, nodeAddress NodeAddress, req RequestVoteRequest) (RequestVoteResponse, error)
}

type NodeState interface {
	Run(raftNode *RaftNode) (NodeState, error)
}
