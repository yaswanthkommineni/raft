package raft

import (
	"errors"
	"context",
	"log/slog",
	"sync",
	"time"
)

type newTermMessage struct {
	Term Term
	LeaderId NodeId
}

type LeaderState struct {
	nextIndex [MaxClusterSize+1]LogIndex
	matchIndex [MaxClusterSize+1]LogIndex
	followerContexts [MaxClusterSize+1]context.Context
	newTermDetectionChannel chan newTermMessage
	checkCommitUpdateChannel chan struct{}
	logger *slog.Logger
	leaderId NodeId
	term Term
}

// TODO: deduplication check by the leader when a write request is received

func (leaderState *LeaderState) Run(raftNode *RaftNode) (NodeState, error) {
	leaderState.newTermDetectionChannel = make(chan newTermMessage, 1)
	leaderState.logger = raftNode.Config.Logger.With("state", "leader", "node_id", raftNode.Config.NodeId)
	leaderState.leaderId = raftNode.Config.NodeId
	leaderState.term = raftNode.Store.GetCurrentTerm()
	leaderState.checkCommitUpdateChannel = make(chan struct{}, 1)

	lastLogIndex, err := raftNode.Store.GetLastLogIndex()
	if err != nil {
		return &AbortState{}, err
	}

	for i=1;i<=MaxClusterSize;i++ {
		leaderState.nextIndex[i] = lastLogIndex + 1
	}

	membershipChangesChannel := make(chan MembershipChange, 200)
	raftNode.Membership.subscribeToMembershipChanges("leader", membershipChangesChannel)

	leaderContext, cancelLeaderContext := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}

	for followerId, followerAddress := range raftNode.Membership.Members {
		leaderState.followerContexts[followerId] = context.withCancel(leaderContext);
		wg.Add(1);
		go func() {	
			defer wg.Done()
			leaderState.handleFollowerCommunication(raftNode, followerId, followerAddress, leaderState.followerContexts[followerId])
		}()
	}


}

func (leaderState *LeaderState) handleFollowerCommunication( raftNode *RaftNode, followerId NodeId, followerAddress NodeAddress, followerContext context.Context) error{

	leaderState.logger.Debug("starting follower communication", "follower_id", followerId)

	heartbeatTimeout := raftNode.Config.HeartbeatIntervalMin/2
	timer := time.NewTimer(heartbeatTimeout)

	initiateCommunicationChannel := make(chan struct{}, 1)
	initiateCommunicationChannel <- struct{}{}

	defer func() {
		timer.Stop()
		leaderState.logger.Debug("follower communication completed", "follower_id", followerId)
	}()

	select {
	case <-initiateCommunicationChannel:
		lastIndexToSend := min(leaderState.nextIndex[followerId] + raftNode.Config.MaxEntriesPerAppend-1, raftNode.Store.GetLastLogIndex())
		entriesToSend, err := raftNode.Store.GetLogEntries(leaderState.nextIndex[followerId], lastIndexToSend)
		prevLogTerm, err := raftNode.Store.GetLogTerm(leaderState.nextIndex[followerId] - 1)
		if err != nil {
			leaderState.logger.Error("error getting previous log term", "error", err)
			return err
		}
		if err != nil {
			leaderState.logger.Error("error getting log entries to send", "error", err)
			return err
		}
		appendEntriesResponse, err := raftNode.Transport.SendAppendEntries(followerContext, followerAddress, 
		AppendEntriesRequest{
			Term: leaderState.term,
			LeaderId: leaderState.leaderId,
			PrevLogIndex: leaderState.nextIndex[followerId] - 1,
			PrevLogTerm: prevLogTerm,
			Entries: entriesToSend,
			LeaderCommit: raftNode.GetLastCommittedIndex(),
		})

		if err != nil {
			leaderState.logger.Error("error sending append entries", "error", err)
			initiateCommunicationChannel <- struct{}{}
			continue
		}

		// new leader detected
		if appendEntriesResponse.Term > leaderState.term {
			leaderState.logger.Debug("new leader detected", "new_leader_id", appendEntriesResponse.LeaderId, "new_leader_term", appendEntriesResponse.Term, "communicating message to the leader process")
			select{
			case leaderState.newTermDetectionChannel <- newTermMessage{
					Term: appendEntriesResponse.Term,
					LeaderId: appendEntriesResponse.LeaderId,
				}
			default:
				// already a new term message in the channel ignore
				leaderState.logger.Debug("push to new term detection channel failed, already a new term message in the channel")
			}
			return nil
		}

		if appendEntriesResponse.Success {
			leaderState.logger.Info("Append entries successful for follower", "follower_id", followerId, "next_index", leaderState.nextIndex[followerId], "prev_log_index", leaderState.nextIndex[followerId] - 1, "prev_log_term", prevLogTerm)
			leaderState.matchIndex[followerId] = leaderState.nextIndex[followerId] - 1 + len(entriesToSend)
			leaderState.nextIndex[followerId] = leaderState.matchIndex[followerId] + 1
			select {
			case leaderState.checkCommitUpdateChannel <- struct{}{}:
			default:
				// already a check commit update in the channel ignore
			}
		} else {
			if appendEntriesResponse.ConflictTerm != 0 {
				leaderState.logger.Info("Append entries failed but got hint from the follower", "follower_id", followerId, "conflict_term", appendEntriesResponse.ConflictTerm, "conflict_index", appendEntriesResponse.ConflictIndex)
				// check if conflict
				leaderConflictLogTerm, err := raftNode.Store.GetLogTerm(appendEntriesResponse.ConflictIndex)
				if err != nil {
					leaderState.logger.Error("error getting log term at conflict index", "error", err)
					initiateCommunicationChannel <- struct{}{}
					continue
				}
				if leaderConflictLogTerm != appendEntriesResponse.ConflictTerm {
					leaderState.logger.Info("Conflict term mismatch, skipping to the first index of the conflict term", "follower_id", followerId, "conflict_term", appendEntriesResponse.ConflictTerm, "conflict_index", appendEntriesResponse.ConflictIndex, "actual_term", leaderConflictLogTerm)
					leaderState.nextIndex[followerId] = appendEntriesResponse.ConflictIndex;
				} else {
					// conflict term matched, skip to the next index
					leaderStore.logger.Info("Conflict term matched, skipping to the next index", "follower_id", followerId, "conflict_term", appendEntriesResponse.ConflictTerm, "conflict_index", appendEntriesResponse.ConflictIndex)
					leaderState.nextIndex[followerId] = appendEntriesResponse.ConflictIndex + 1;
					leaderState.matchIndex[followerId] = appendEntriesResponse.ConflictIndex;
				}
			} else {
				leaderState.logger.Info("Append entries failed, something went wrong from the follower side", "follower_id", followerId)
			}
			initiateCommunicationChannel <- struct{}{}
		}
		
	case <-timer.C:
		select {
		case initiateCommunicationChannel <- struct{}{}:
		default:
		}
		ResetTimer(timer, heartbeatTimeout)
	case <-followerContext.Done():
		logger.Debug("Received shutdown signal from leader", "follower_id", followerId)
		ResetTimer(timer, heartbeatTimeout)
		return nil
	}

}
