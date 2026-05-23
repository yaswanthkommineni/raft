package raft

// This file contains the basic types used in the Raft implementation.
import (
	"errors"
	"fmt"
)

type Term uint64

type LogIndex uint64

type LogType uint8

type NodeAddress string

type NodeId uint64

type ClientId string

type SequenceNum uint64

type MembershipChange struct {
	LogIndex LogIndex
	NodeId NodeId
	NodeAddress NodeAddress
	IsNodeRemoval bool
	Confirmation bool
	// true if the change is to confirm an already existing membership change ((new) from (old + new))
}

type Node struct {
	NodeId  NodeId
	Address NodeAddress
}

type LogEntry struct {
	Term        Term
	LogIndex    LogIndex
	LogType     LogType
	Data        []byte
	ClientId    ClientId
	SequenceNum SequenceNum
}

type AppendEntriesRequest struct {
	Term         Term
	LeaderId     NodeId
	PrevLogIndex LogIndex
	PrevLogTerm  Term
	Entries      []LogEntry
	LeaderCommit LogIndex
}

type AppendEntriesResponse struct {
	Term          Term
	Success       bool
	ConflictTerm  Term
	ConflictIndex LogIndex
}

type RequestVoteRequest struct {
	Term         Term
	CandidateId  NodeId
	LastLogIndex LogIndex
	LastLogTerm  Term
}

type RequestVoteResponse struct {
	Term        Term
	VoteGranted bool
}

type ClientRequest struct {
	ClientId    ClientId
	SequenceNum SequenceNum
	Data        []byte
}

type ClientResponse struct {
	Success  bool
	Data     []byte
	LeaderId NodeId // for redirect
}

// Envelope types wrap a request with a response channel for async processing.
type AppendEntriesEnvelope struct {
	Req    AppendEntriesRequest
	RespCh chan AppendEntriesResponse
}

type RequestVoteEnvelope struct {
	Req    RequestVoteRequest
	RespCh chan RequestVoteResponse
}

type ClientRequestEnvelope struct {
	Req    ClientRequest
	RespCh chan ClientResponse
}

// StateMachineResponseData represents the result of applying a log entry to the state machine. It can be any byte slice, allowing for flexibility in the type of data returned by the state machine.
type StateMachineResponseData []byte

// abort state
type AbortState struct {
}

func (abortState *AbortState) Run(raftNode *RaftNode) (NodeState, error) {
	return nil, errors.New("AbortState")
}

// LogIndexOutOfBoundsError
type LogIndexOutOfBoundsError struct {
}

func (logIndexOutOfBoundsError LogIndexOutOfBoundsError) Error() string {
	return fmt.Sprintf("Log index out of bounds")
}
