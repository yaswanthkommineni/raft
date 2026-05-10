package raft;

import (
	"context"
	"sync/atomic"
	"sync"
)

type CandidateState struct {
	// the number of votes can be fit into 32 bit integer
	newClusterVotesReceived, oldClusterVotesReceived atomic.Uint32;
	newClusterSize, oldClusterSize uint32;
	declareWin chan NodeId;
}

func (candidateState *CandidateState) Run(raftNode *RaftNode) (NodeState, error) {

	raftNode.SetLeaderId(0);
	raftNode.Store.SetVotedFor(raftNode.Config.NodeId);
	raftNode.Store.SetCurrentTerm(raftNode.Store.GetCurrentTerm() + 1);

	candidateState.declareWin = make(chan NodeId, MaxClusterSize + 1);

	candidateState.oldClusterSize = uint32(len(raftNode.Membership.Members));
	candidateState.newClusterSize = uint32(len(raftNode.Membership.Members));

	wg := sync.WaitGroup{};

	if raftNode.Membership.IsNodeRemoval {
		candidateState.newClusterSize -= 1;
	} else {
		candidateState.oldClusterSize -= 1;
	}

	candidateState.incrementAndCheckVotes(raftNode, raftNode.Config.NodeId);

	electionTimeout := RandomDuration(raftNode.Config.ElectionTimeoutMin, raftNode.Config.ElectionTimeoutMax);
	cadidateContext, cancelCandidateContext := context.WithTimeout(context.Background(), electionTimeout);
	defer cancelCandidateContext();

	// start go-routines to request votes from other nodes

	raftNode.Membership.forEachNode(func(nodeId NodeId, nodeAddress NodeAddress) {
		if nodeId == raftNode.Config.NodeId {
			return;
		}
		
		wg.Add(1);
		go func() {
			voteRequestContext, cancelVoteRequestContext := context.WithTimeout(candidateContext, raftNode.Config.RequestVoteTimeout);
			defer func () {
				cancelVoteRequestContext();
				wg.Done();
			}();
			for {
				voteResponse, err := requestVote(voteRequestContext, nodeId, raftNode);
				
				if err != nil {
					select {
					case <- candidateContext.Done():
						return;
					default:
					}
					// to-do: encountered some error, log it
					// retrying...
					cancelVoteRequestContext();
					voteRequestContext, cancelVoteRequestContext = context.WithTimeout(candidateContext, raftNode.Config.RequestVoteTimeout);
					continue;
				}
				if voteResponse.VoteGranted {
					candidateState.incrementAndCheckVotes(raftNode, nodeId);
				} else {
					if voteResponse.Term > raftNode.Store.GetCurrentTerm() {
						raftNode.Store.SetCurrentTerm(voteResponse.Term);
						raftNode.Store.SetVotedFor(0);
						candidateState.declareWin <- 0;
					}
				}
				return;
			}
		}();

	});

	var nextState NodeState;

	breakLoop := false;

	for !breakLoop {
		select {
		case winnerId := <- candidateState.declareWin:
			cancelCandidateContext();
			raftNode.SetLeaderId(winnerId);
			if winnerId == raftNode.Config.NodeId {
				nextState = LeaderState{};
			} else {
				nextState = FollowerState{};
			}
			breakLoop = true;
		case <- raftNode.ShutdownCh:
			cancelCandidateContext();
			nextState = AbortState{};
			breakLoop = true;
		case req := <- raftNode.RequestVoteCh:
			if req.Req.Term > raftNode.Store.GetCurrentTerm() {
				cancelCandidateContext();
				raftNode.Store.SetCurrentTerm(req.Req.Term);
				raftNode.Store.SetVotedFor(0);
				candidateState.declareWin <- 0;
				// the next state will decide to grant or reject the vote based on the log comparision
				go func() {
					// to avoid deadlock
					raftNode.RequestVoteCh <- req;
				}();
				breakLoop = true;
				continue;
			}
			// we are free to reject the vote request
			req.RespCh <- RequestVoteResponse{
				Term: raftNode.Store.GetCurrentTerm(),
				VoteGranted: false,
			}
		case req := <- raftNode.AppendEntriesCh:
			if req.Req.Term >= raftNode.Store.GetCurrentTerm() {
				cancelCandidateContext();
				if req.Req.Term > raftNode.Store.GetCurrentTerm() {
					raftNode.Store.SetCurrentTerm(req.Req.Term);
					raftNode.Store.SetVotedFor(0);
				}
				candidateState.declareWin <- req.Req.LeaderId;
				// the next state will handle the AppendEntries request
				go func() {
					// to avoid deadlock
					raftNode.AppendEntriesCh <- req;
				}();
				breakLoop = true;
				continue;
			}
			// we are free to reject the AppendEntries request
			req.RespCh <- AppendEntriesResponse{
				Term: raftNode.Store.GetCurrentTerm(),
				Success: false,
			}
		case <- cadidateContext.Done():
			// election timeout, start new election
			if cadidateContext.Err() != context.DeadlineExceeded {
				// to-do: log the error
				nextState = AbortState{};
				breakLoop = true;
				continue;
			}
			raftNode.Store.SetVotedFor(0);
			raftNode.SetLeaderId(0);
			nextState = CandidateState{};
			breakLoop = true;
		}
	}

	wg.Wait();
	return nextState, nil;
}



func (candidateState *CandidateState) incrementAndCheckVotes(raftNode *RaftNode, votedNode NodeId) {
	if raftNode.Membership.ChangeNode == votedNode {
		if raftNode.Membership.IsNodeRemoval {
			atomic.AddUint32(&candidateState.oldClusterVotesReceived, 1);
		} else {
			atomic.AddUint32(&candidateState.newClusterVotesReceived, 1);
		}
	} else {
		atomic.AddUint32(&candidateState.oldClusterVotesReceived, 1);
		atomic.AddUint32(&candidateState.newClusterVotesReceived, 1);
	}
	if checkMajority(candidateState.oldClusterSize, atomic.LoadUint32(&candidateState.oldClusterVotesReceived)) && checkMajority(candidateState.newClusterSize, atomic.LoadUint32(&candidateState.newClusterVotesReceived)) {
		candidateState.declareWin <- raftNode.Config.NodeId;
	}
}

func checkMajority(totalNodes uint32, votesReceived uint32) bool {
	return votesReceived > (totalNodes/2);
}

func requestVote(ctx context.Context, followerId NodeId, raftNode *RaftNode) (RequestVoteResponse, error) {
	return raftNode.Transport.SendRequestVote(ctx, raftNode.Membership[followerId], RequestVoteRequest{
		Term : raftNode.Store.GetCurrentTerm(),
		CandidateId : raftNode.Config.NodeId,
		LastLogIndex : raftNode.Store.GetLastLogIndex(),
		LastLogTerm : raftNode.Store.GetLastLogTerm(),
	});
}