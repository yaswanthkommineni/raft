package raft;
// This file contains the interfaces that the Raft implementation will use.

// Not an external system inteface, but an internal one to abstract the Raft node's state and behavior. This allows for better modularity and separation of concerns in the implementation. 
// The Store interface defines the methods that a storage backend must implement to be used by the Raft node. This allows for flexibility in choosing different storage implementations, such as in-memory or disk-based storage.
// The store is dumb the ones who are using this should deal with the logic
type Store interface {
	// State persistence - term and votedFor
	GetCurrentTerm() Term;
	GetVotedFor() NodeId;
	SetCurrentTerm(term Term) error;
	SetVotedFor(nodeId NodeId) error;

	// Log operations
	Append(entry *LogEntry) error;
	GetLogEntry(index LogIndex) (*LogEntry, error);
	GetLogEntries(startIndex LogIndex, endIndex LogIndex) ([]*LogEntry, error);
	GetLastLogIndex() LogIndex;
	GetLastLogTerm() Term;
	GetFirstLogIndex(term Term) LogIndex;
	TruncateFrom(index LogIndex) error;
}

// The StateMachine interface defines the methods that a state machine must implement to be used by the Raft node. This allows for flexibility in choosing different state machine implementations, such as a key-value store or a more complex application-specific state machine.
type StateMachine interface {
	Apply (entry *LogEntry) (StateMachineResponseData, error);
}

type NodeState interface {
	Run(raftNode *RaftNode) error;
}