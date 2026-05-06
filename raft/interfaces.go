package raft;
// This file contains the interfaces that the Raft implementation will use.

// Not an external system inteface, but an internal one to abstract the Raft node's state and behavior. This allows for better modularity and separation of concerns in the implementation. 
// The Store interface defines the methods that a storage backend must implement to be used by the Raft node. This allows for flexibility in choosing different storage implementations, such as in-memory or disk-based storage.
// The store is dumb the ones who are using this should deal with the logic
// The implementation of this should be thread safe and atomic => either an operation succeeds or fails, but it should not leave the store in an inconsistent state. This is important for the correctness of the Raft algorithm, as it relies on the consistency of the log and the state of the node.
type Store interface {
	// State persistence - term and votedFor

	// GetCurrentTerm should be served from memory and not the persistent storage, but it should be updated with the persistent storage
	GetCurrentTerm() Term;
	// GetVotedFor should be served from memory and not the persistent storage, but it should be updated with the persistent storage
	GetVotedFor() NodeId;
	SetCurrentTerm(term Term) error;
	SetVotedFor(nodeId NodeId) error;

	// just blindly overwrite or append the entries at their index, the ones who are using this should deal with the logic
	PatchEntries(entries []LogEntry) error;
	// return LogIndexOutOfBoundsError error
	GetLogEntry(index LogIndex) (*LogEntry, error);
	GetLogEntries(startIndex LogIndex, endIndex LogIndex) ([]LogEntry, error);
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
	Run(raftNode *RaftNode) (NodeState, error);
}