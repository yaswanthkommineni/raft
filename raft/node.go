package raft;
// This file contains the implementation of the Raft node.

// The RaftNode struct represents a single node in the Raft cluster. It contains the state of the node, such as its current term, log entries, and other relevant information.
type RaftNode struct {
	StateMachine StateMachine;
	Store Store;
	Transport Transport;
	Membership Membership;

	LastAppliedIndex LogIndex;
	LastCommittedIndex LogIndex;

	// Channels for inbound requests
	AppendEntriesCh chan AppendEntriesEnvelope;
	RequestVoteCh   chan RequestVoteEnvelope;
	ClientRequestCh chan ClientRequestEnvelope;

	NodeState NodeState;
}


func (raftNode* RaftNode) Boot() error {

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

func (raftNode* RaftNode) Run() error {

}

func (raftNode* RaftNode) Shutdown() error {
	
}
