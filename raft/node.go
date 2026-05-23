package raft

import (
	"sync"
	"sync/atomic"
)

// This file contains the implementation of the Raft node.

// The RaftNode struct represents a single node in the Raft cluster. It contains the state of the node, such as its current term, log entries, and other relevant information.
type RaftNode struct {
	Config Config

	StateMachine StateMachine
	Store        Store
	Transport    Transport
	Membership   Membership

	leaderId           NodeId
	LastAppliedIndex   LogIndex
	lastCommittedIndex LogIndex

	// Channels for inbound requests
	AppendEntriesCh chan AppendEntriesEnvelope
	RequestVoteCh   chan RequestVoteEnvelope
	ClientRequestCh chan ClientRequestEnvelope

	wg sync.WaitGroup

	exitErr error

	// no data gets passed on this channel
	ShutdownCh   chan bool
	shutdownOnce sync.Once

	NodeState NodeState

	// storeBreaker is the Store wrapper that tracks consecutive write failures.
	// nil when Config.StoreErrorThreshold == 0 (breaker disabled).
	storeBreaker *CircuitBreakerStore
}

func (n *RaftNode) GetLeaderId() NodeId {
	return NodeId(atomic.LoadUint64((*uint64)(&n.leaderId)))
}

func (n *RaftNode) SetLeaderId(leaderId NodeId) {
	atomic.StoreUint64((*uint64)(&n.leaderId), uint64(leaderId))
}

// implement thread save concurrenctly callable getters and setters for the RaftNode's state, such as the last committed index.
func (n *RaftNode) GetLastCommittedIndex() LogIndex {
}

func (n *RaftNode) SetLastCommittedIndex(index LogIndex) {
}

func (n *RaftNode) Boot() error {

	// By this point, the store should have been initialized with the user implemented store implementation

	// Wrap the store with interpreter to intercept the log entries and detect the membership change type
	n.Store = NewStoreInterpreter(n.Store, &n.Membership, n.Config.Logger)

	// Wrap the user-provided Store with the circuit breaker (or reset the
	// existing wrapper for a re-boot). Every Boot gives the node a fresh
	// counter and an un-tripped breaker.

	if n.Config.StoreErrorThreshold > 0 {
		if existing, ok := n.Store.(*CircuitBreakerStore); ok {
			existing.Reset()
			n.storeBreaker = existing
		} else {
			wrapper := NewCircuitBreakerStore(n.Store, n.Config.StoreErrorThreshold, n.Config.Logger.With("node_id", n.Config.NodeId))
			n.Store = wrapper
			n.storeBreaker = wrapper
		}
	} else {
		n.storeBreaker = nil
	}
	// TODO: set the NodeState (initial state on cluster join / bootstrap)
	return nil
}

// HandleAppendEntries is called by the transport when an AppendEntries RPC arrives.
// It pushes the request onto the channel and blocks until the state loop responds.
func (n *RaftNode) HandleAppendEntries(req AppendEntriesRequest) AppendEntriesResponse {
	envelope := AppendEntriesEnvelope{
		Req:    req,
		RespCh: make(chan AppendEntriesResponse, 1),
	}
	n.AppendEntriesCh <- envelope
	return <-envelope.RespCh
}

//TODO: Implement shutting down all the goroutines that are handling the requests when abort is hit

// HandleRequestVote is called by the transport when a RequestVote RPC arrives.
func (n *RaftNode) HandleRequestVote(req RequestVoteRequest) RequestVoteResponse {
	envelope := RequestVoteEnvelope{
		Req:    req,
		RespCh: make(chan RequestVoteResponse, 1),
	}
	n.RequestVoteCh <- envelope
	return <-envelope.RespCh
}

// Submit is called by the client-facing transport for read/write requests.
func (n *RaftNode) Submit(req ClientRequest) ClientResponse {
	envelope := ClientRequestEnvelope{
		Req:    req,
		RespCh: make(chan ClientResponse, 1),
	}
	n.ClientRequestCh <- envelope
	return <-envelope.RespCh
}

func (n *RaftNode) Run() error {
	n.wg.Add(1)
	go func() {
		for {
			nextState, err := n.NodeState.Run(n)
			select {
			case <-n.ShutdownCh:
				n.wg.Done()
				return
			default:
			}
			// Circuit breaker check happens between state transitions: if the
			// Store has accumulated too many consecutive write failures, force
			// abort regardless of what the state returned.
			if n.storeBreaker != nil && n.storeBreaker.Tripped() {
				n.Config.Logger.Error("store circuit breaker tripped; aborting node")
				n.exitErr = err
				n.wg.Done()
				n.Shutdown()
				return
			}
			if _, abort := nextState.(*AbortState); abort {
				// Abort is the only fatal signal. err is carried out via exitErr
				// for the caller (Shutdown) to inspect; states that return a
				// non-nil err with a non-Abort next state must already have
				// logged the error themselves.
				n.exitErr = err
				n.wg.Done()
				n.Shutdown()
				return
			}
			if err != nil {
				n.Config.Logger.Error("Last state returned an error:", err, "continuing with next state")
			}
			n.NodeState = nextState
		}
	}()
	return nil
}

// Only graceful shutdown is supported for now
func (n *RaftNode) Shutdown() error {
	n.shutdownOnce.Do(func() { close(n.ShutdownCh) })
	// implement other graceful shutdown steps here
	n.wg.Wait()
	if n.exitErr != nil {
		return n.exitErr
	}
	// log that shutdown happened in expected pattern
	return nil
}
