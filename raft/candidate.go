package raft

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

// electionResult carries the resolved outcome of an election attempt:
//   - winnerId == self  → we won this term
//   - winnerId == 0     → step down (a higher term was observed)
//   - winnerId == other → a valid leader was observed for >= our term
type electionResult struct {
	winnerId   NodeId
	winnerTerm Term
}

type CandidateState struct {
	// vote counters per cluster configuration (joint consensus uses both)
	newClusterVotesReceived, oldClusterVotesReceived uint32
	newClusterSize, oldClusterSize                   uint32
	electionResultCh                                 chan electionResult
	electionTerm                                     Term
}

func (candidateState *CandidateState) Run(raftNode *RaftNode) (NodeState, error) {
	logger := raftNode.Config.Logger.With(
		"state", "candidate",
		"node_id", raftNode.Config.NodeId,
	)

	raftNode.SetLeaderId(0)
	newTerm := raftNode.Store.GetCurrentTerm() + 1
	// Atomic operation on term and votedFor
	if err := raftNode.Store.SetState(newTerm, raftNode.Config.NodeId); err != nil {
		return &CandidateState{}, err
	}

	candidateState.electionTerm = raftNode.Store.GetCurrentTerm()
	candidateState.electionResultCh = make(chan electionResult, MaxClusterSize+1)
	candidateState.oldClusterSize = uint32(len(raftNode.Membership.Members))
	candidateState.newClusterSize = uint32(len(raftNode.Membership.Members))

	if raftNode.Membership.ChangeNode != 0 {
		if raftNode.Membership.IsNodeRemoval {
			candidateState.newClusterSize -= 1
		} else {
			candidateState.oldClusterSize -= 1
		}
	}

	logger = logger.With("term", candidateState.electionTerm)
	logger.Info("election started",
		"old_cluster_size", candidateState.oldClusterSize,
		"new_cluster_size", candidateState.newClusterSize,
	)

	// Snapshot log position once for this election — every RequestVote sent
	// to peers carries these same values, so there's no point re-reading per goroutine.
	lastLogIndex, err := raftNode.Store.GetLastLogIndex()
	if err != nil {
		return &AbortState{}, err
	}
	lastLogTerm, err := raftNode.Store.GetLastLogTerm()
	if err != nil {
		return &AbortState{}, err
	}
	voteRequest := RequestVoteRequest{
		Term:         candidateState.electionTerm,
		CandidateId:  raftNode.Config.NodeId,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	candidateState.incrementAndCheckVotes(raftNode, raftNode.Config.NodeId, logger)

	wg := sync.WaitGroup{}
	electionTimeout := RandomDuration(raftNode.Config.ElectionTimeoutMin, raftNode.Config.ElectionTimeoutMax)
	candidateContext, cancelCandidateContext := context.WithTimeout(context.Background(), electionTimeout)
	defer cancelCandidateContext()

	// Per-peer vote goroutines. Transport implementations retry transient
	// failures internally until ctx is canceled. The election timeout is the
	// per-peer budget — a slow peer keeps getting retried until candidateContext
	// expires, maximizing the chance of collecting that vote.
	raftNode.Membership.forEachNode(func(nodeId NodeId, nodeAddress NodeAddress) {
		if nodeId == raftNode.Config.NodeId {
			return
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			peerLogger := logger.With("peer_id", nodeId)

			voteResponse, err := requestVote(candidateContext, nodeId, raftNode, voteRequest)
			if err != nil {
				if candidateContext.Err() != nil {
					peerLogger.Debug("vote request aborted: election ended", "error", err)
				} else {
					peerLogger.Warn("vote request failed", "error", err)
				}
				return
			}

			if voteResponse.VoteGranted {
				peerLogger.Debug("vote granted")
				candidateState.incrementAndCheckVotes(raftNode, nodeId, peerLogger)
				return
			}

			peerLogger.Debug("vote rejected", "peer_term", voteResponse.Term)
			if voteResponse.Term > candidateState.electionTerm {
				peerLogger.Info("higher term observed in vote rejection", "peer_term", voteResponse.Term)
				// There is no point in waiting. Only the first element in the elctionResultCh will be read.
				select {
				case candidateState.electionResultCh <- electionResult{winnerId: 0, winnerTerm: voteResponse.Term}:
				default:
				}
			}
		}()
	})

	var nextState NodeState
	var errReturned error

loop:
	for {
		select {
		case result := <-candidateState.electionResultCh:
			cancelCandidateContext()
			raftNode.SetLeaderId(result.winnerId)
			if result.winnerTerm > candidateState.electionTerm {
				if err := raftNode.Store.SetState(result.winnerTerm, 0); err != nil {
					nextState = &AbortState{}
					errReturned = err
					break loop
				}
			}
			if result.winnerId == raftNode.Config.NodeId {
				logger.Info("election won", "won_term", result.winnerTerm)
				// TODO: LeaderState type is not defined yet (leader.go is empty).
				// Once defined with a pointer-receiver Run, &LeaderState{} will compile.
				nextState = &LeaderState{}
			} else {
				logger.Info("election lost; stepping down to follower",
					"leader_id", result.winnerId, "leader_term", result.winnerTerm)
				nextState = &FollowerState{}
			}
			break loop

		case <-raftNode.ShutdownCh:
			cancelCandidateContext()
			logger.Info("shutdown received during election")
			nextState = &AbortState{}
			break loop

		case req := <-raftNode.RequestVoteCh:
			if req.Req.Term > candidateState.electionTerm {
				logger.Info("higher-term RequestVote during election; stepping down",
					"peer_id", req.Req.CandidateId, "peer_term", req.Req.Term)
				select {
				case candidateState.electionResultCh <- electionResult{winnerId: 0, winnerTerm: req.Req.Term}:
				default:
				}
				go func() {
					select {
					case raftNode.RequestVoteCh <- req:
						// Don't wait if already full. Else it will become a leak incase of abort just after the goroutine is started.
					default:
						req.RespCh <- RequestVoteResponse{
							Term: candidateState.electionTerm,
							VoteGranted: false,
						}
					}
				}()
				continue
			}
			logger.Debug("rejecting RequestVote at lower-or-equal term",
				"peer_id", req.Req.CandidateId, "peer_term", req.Req.Term)
			req.RespCh <- RequestVoteResponse{
				Term:        candidateState.electionTerm,
				VoteGranted: false,
			}

		case req := <-raftNode.AppendEntriesCh:
			if req.Req.Term >= candidateState.electionTerm {
				logger.Info("leader observed during election; stepping down",
					"leader_id", req.Req.LeaderId, "leader_term", req.Req.Term)
				select {
				case candidateState.electionResultCh <- electionResult{winnerId: req.Req.LeaderId, winnerTerm: req.Req.Term}:
				default:
				}
				go func() {
					select {
					case raftNode.AppendEntriesCh <- req:
					default:
						req.RespCh <- AppendEntriesResponse{
							Term: candidateState.electionTerm,
							Success: false,
						}
					}
				}()
				continue
			}
			logger.Debug("rejecting stale AppendEntries",
				"leader_id", req.Req.LeaderId, "leader_term", req.Req.Term)
			req.RespCh <- AppendEntriesResponse{
				Term:    candidateState.electionTerm,
				Success: false,
			}

		case <-candidateContext.Done():
			/*
				This part of the code is never reached because when the context is cancelled the loop is broken.
				But the context is cancelled to stope the API requesting votes are needed to be stopped.
			*/
			// if candidateContext.Err() != context.DeadlineExceeded {
			// 	logger.Warn("candidate context ended unexpectedly; aborting",
			// 		"error", candidateContext.Err())
			// 	nextState = &AbortState{}
			// 	break loop
			// }
			logger.Info("election timed out; starting a new one")
			raftNode.SetLeaderId(0)
			nextState = &CandidateState{}
			break loop
		}
	}

	wg.Wait()
	return nextState, errReturned
}

func (candidateState *CandidateState) incrementAndCheckVotes(raftNode *RaftNode, votedNode NodeId, logger *slog.Logger) {
	if raftNode.Membership.ChangeNode == votedNode {
		if raftNode.Membership.IsNodeRemoval {
			atomic.AddUint32(&candidateState.oldClusterVotesReceived, 1)
		} else {
			atomic.AddUint32(&candidateState.newClusterVotesReceived, 1)
		}
	} else {
		atomic.AddUint32(&candidateState.oldClusterVotesReceived, 1)
		atomic.AddUint32(&candidateState.newClusterVotesReceived, 1)
	}
	oldVotes := atomic.LoadUint32(&candidateState.oldClusterVotesReceived)
	newVotes := atomic.LoadUint32(&candidateState.newClusterVotesReceived)
	logger.Debug("vote counted", "voter_id", votedNode, "old_votes", oldVotes, "new_votes", newVotes)
	if checkMajority(candidateState.oldClusterSize, oldVotes) && checkMajority(candidateState.newClusterSize, newVotes) {
		select {
		case candidateState.electionResultCh <- electionResult{winnerId: raftNode.Config.NodeId, winnerTerm: candidateState.electionTerm}:
		default:
		}
	}
}

func checkMajority(totalNodes uint32, votesReceived uint32) bool {
	return votesReceived > (totalNodes / 2)
}

func requestVote(ctx context.Context, followerId NodeId, raftNode *RaftNode, req RequestVoteRequest) (RequestVoteResponse, error) {
	return raftNode.Transport.SendRequestVote(ctx, raftNode.Membership.Members[followerId], req)
}
