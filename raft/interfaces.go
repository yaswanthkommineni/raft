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

	// Could also get an empty slice, if so don't return error
	// just blindly overwrite or append the entries at their index, the ones who are using this should deal with the logic
	// multiple calls can be made to this function but on different indexes, so this should be thread safe
	PatchEntries(entries []LogEntry) error;
	// return LogIndexOutOfBoundsError error
	GetLogEntry(index LogIndex) (*LogEntry, error);
	GetLogTerm(index LogIndex) (Term, error);
	GetLogEntries(startIndex LogIndex, endIndex LogIndex) ([]LogEntry, error);
	GetLastLogIndex() LogIndex;
	GetLastLogTerm() Term;
	GetFirstLogIndex(term Term) LogIndex;
	// truncate last ones first
	TruncateFrom(index LogIndex) error;
}

// The StateMachine interface defines the methods that a state machine must implement to be used by the Raft node. This allows for flexibility in choosing different state machine implementations, such as a key-value store or a more complex application-specific state machine.
type StateMachine interface {
	Apply (entry *LogEntry) (StateMachineResponseData, error);
}

// Transport abstracts the network layer. The algorithm only knows NodeIds;
// each Transport implementation maintains its own address book (NodeId → address).
// All methods must be safe for concurrent use.
type Transport interface {
	// SendAppendEntries sends an AppendEntries RPC to the given node.
	// The context allows the caller to cancel in-flight RPCs (e.g., on shutdown or state transition).
	SendAppendEntries(ctx context.Context, nodeAddress NodeAddress, req AppendEntriesRequest) (AppendEntriesResponse, error);

	// SendRequestVote sends a RequestVote RPC to the given node.
	SendRequestVote(ctx context.Context, nodeAddress NodeAddress, req RequestVoteRequest) (RequestVoteResponse, error);
}

type NodeState interface {
	Run(raftNode *RaftNode) (NodeState, error);
}