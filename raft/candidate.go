package raft;

import (
	"context"
	"sync/atomic"
	"sync"
)

type CandidateState struct {
	// the number of votes can be fit into 32 bit integer
	newClusterVotesReceived, oldClusterVotesReceived uint32;
	newClusterSize, oldClusterSize uint32;
	// carries the resolved leader for this election: candidate's own id on a win,
	// 0 on a step-down (higher term observed), or the leader id on AppendEntries from a valid leader
	electionResultCh chan struct{winnerId NodeId; winnerTerm Term};
	electionTerm Term;
}

func (candidateState *CandidateState) Run(raftNode *RaftNode) (NodeState, error) {

	raftNode.SetLeaderId(0);
	raftNode.Store.SetCurrentTerm(raftNode.Store.GetCurrentTerm() + 1);
	raftNode.Store.SetVotedFor(raftNode.Config.NodeId);

	candidateState.electionResultCh = make(chan struct{winnerId NodeId; winnerTerm Term}, MaxClusterSize + 1);
	candidateState.electionTerm = raftNode.Store.GetCurrentTerm();
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
	candidateContext, cancelCandidateContext := context.WithTimeout(context.Background(), electionTimeout);
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
			// TODO: bound this retry loop with a max-attempt count or exponential backoff;
			// today it spins until candidateContext expires.
			for {
				voteResponse, err := requestVote(voteRequestContext, nodeId, raftNode, candidateState.electionTerm);

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
					if voteResponse.Term > candidateState.electionTerm {
						candidateState.electionResultCh <- struct{winnerId: 0; winnerTerm: voteResponse.Term};
					}
				}
				return;
			}
		}();

	});

	var nextState NodeState;

loop:
	for {
		select {
		case result := <- candidateState.electionResultCh:
			cancelCandidateContext();
			raftNode.SetLeaderId(result.winnerId);
			if(result.winnerTerm > candidateState.electionTerm){
				raftNode.Store.SetCurrentTerm(result.winnerTerm);
				raftNode.Store.SetVotedFor(0);
			}
			if result.winnerId == raftNode.Config.NodeId {
				nextState = &LeaderState{};
			} else {
				nextState = &FollowerState{};
			}
			break loop;
		case <- raftNode.ShutdownCh:
			cancelCandidateContext();
			nextState = &AbortState{};
			break loop;
		// higher term observed, exit
		case req := <- raftNode.RequestVoteCh:
			if req.Req.Term > candidateState.electionTerm {
				candidateState.electionResultCh <- struct{winnerId: 0; winnerTerm: req.Req.Term};
				// TODO: spawning a goroutine to re-enqueue leaks if the next state never reads RequestVoteCh
				// (e.g. AbortState). Replace with a buffered handoff slot on RaftNode that the next state drains first.
				// the next state will decide to grant or reject the vote based on the log comparision
				go func() {
					// to avoid deadlock
					raftNode.RequestVoteCh <- req;
				}();
				continue;
			}
			// we are free to reject the vote request
			req.RespCh <- RequestVoteResponse{
				Term: candidateState.electionTerm,
				VoteGranted: false,
			}
		// new leader observed, exit
		case req := <- raftNode.AppendEntriesCh:
			if req.Req.Term >= candidateState.electionTerm {
				candidateState.electionResultCh <- struct{winnerId: req.Req.LeaderId; winnerTerm: req.Req.Term};
				// TODO: spawning a goroutine to re-enqueue leaks if the next state never reads AppendEntriesCh.
				// Replace with a buffered handoff slot on RaftNode that the next state drains first.
				// the next state will handle the AppendEntries request
				go func() {
					// to avoid deadlock
					raftNode.AppendEntriesCh <- req;
				}();
				// don't break the loog, the one who processes the electionResultCh will break it
				continue;
			}
			// we are free to reject the AppendEntries request
			req.RespCh <- AppendEntriesResponse{
				Term: candidateState.electionTerm,
				Success: false,
			}
		case <- candidateContext.Done():
			// election timeout, start new election
			if candidateContext.Err() != context.DeadlineExceeded {
				// to-do: log the error
				nextState = &AbortState{};
				break loop;
			}
			raftNode.Store.SetVotedFor(0);
			raftNode.SetLeaderId(0);
			nextState = &CandidateState{};
			break loop;
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
		candidateState.electionResultCh <- struct{winnerId: raftNode.Config.NodeId; winnerTerm: candidateState.electionTerm};
	}
}

func checkMajority(totalNodes uint32, votesReceived uint32) bool {
	return votesReceived > (totalNodes/2);
}

func requestVote(ctx context.Context, followerId NodeId, raftNode *RaftNode, electionTerm Term) (RequestVoteResponse, error) {
	return raftNode.Transport.SendRequestVote(ctx, raftNode.Membership.Members[followerId], RequestVoteRequest{
		Term : electionTerm,
		CandidateId : raftNode.Config.NodeId,
		LastLogIndex : raftNode.Store.GetLastLogIndex(),
		LastLogTerm : raftNode.Store.GetLastLogTerm(),
	});
}