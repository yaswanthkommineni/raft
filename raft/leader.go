package raft

import (
	"context"
	"log/slog"
	"slices"
	"strconv"
	"sync"
	"time"
)

type newTermMessage struct {
	Term     Term
	LeaderId NodeId
}

type LeaderState struct {
	nextIndex                [MaxClusterSize + 1]LogIndex
	matchIndex               [MaxClusterSize + 1]LogIndex
	followerContextsCancel   [MaxClusterSize + 1]context.CancelFunc
	newTermDetectionChannel  chan newTermMessage
	checkCommitUpdateChannel chan struct{}
	logger                   *slog.Logger
	leaderId                 NodeId
	term                     Term
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

	for i := 1; i <= MaxClusterSize; i++ {
		leaderState.nextIndex[i] = lastLogIndex + 1
	}

	startIndex, err := raftNode.Store.GetLastLogIndex()

	if err != nil {
		leaderState.logger.Error("error getting last log index", "error", err)
		return &AbortState{}, err
	}

	startIndex += 1

	// no-op entry to commit the previous entries in the log
	err = raftNode.Store.PatchEntries([]LogEntry{
		{
			Term:     leaderState.term,
			LogIndex: startIndex,
			LogType:  LogTypeNoOp,
		},
	})

	if err != nil {
		leaderState.logger.Error("error appending no-op entry to log", "error", err)
		return &AbortState{}, err
	}

	membershipChangesChannel := make(chan MembershipChange, 200)
	raftNode.Membership.subscribeToMembershipChanges("leader", membershipChangesChannel)

	leaderContext, cancelLeaderContext := context.WithCancel(context.Background())

	wg := sync.WaitGroup{}

	for followerId, followerAddress := range raftNode.Membership.Members {
		var followerContext context.Context
		followerContext, leaderState.followerContextsCancel[followerId] = context.WithCancel(leaderContext)
		wg.Add(1)
		go func() {
			defer wg.Done()
			leaderState.handleFollowerCommunication(raftNode, followerId, followerAddress, followerContext)
		}()
	}

	defer func() {
		cancelLeaderContext()
		wg.Wait()
		raftNode.Membership.unsubscribeFromMembershipChanges("leader")
		leaderState.logger.Debug("leader state completed")
	}()

	abortSignalChannel := make(chan struct{}, 1)


	// separate goroutine to handle the client requests
	go func () {
		wg.Add(1)
		defer func () {
			wg.Done()
		}()
		for {
			select {
			case envelope := <-raftNode.ClientRequestCh:
				lastLogIndex, err := raftNode.Store.GetLastLogIndex()
				failed := false
				if err != nil {
					leaderState.logger.Error("error getting last log index", "error", err)
					failed = true
				} else {
					// only this goroutine can append to the log and update the lastLogIndex, so no race condition here
					err = raftNode.Store.PatchEntries([]LogEntry{{
						Term:        leaderState.term,
						LogIndex:    lastLogIndex + 1,
						LogType:     LogTypeData,
						Data:        envelope.Req.Data,
						ClientId:    envelope.Req.ClientId,
						SequenceNum: envelope.Req.SequenceNum,
					}})
					if err != nil {
						leaderState.logger.Error("error appending client request to log", "error", err)
						failed = true
					} else {
						leaderState.logger.Info("client request appended to log successfull", "client_id", envelope.Req.ClientId, "sequence_num", envelope.Req.SequenceNum)
						raftNode.RegisterCallbackOnLogApply(lastLogIndex+1, func(stateMachineResponse StateMachineResponseData, err error) error {
							if err != nil {
								leaderState.logger.Error("error calling callback on log apply", "error", err)
								envelope.RespCh <- ClientResponse{
									Success:      false,
									ErrorMessage: "Error applying log entry to the state machine",
									ErrorCode:    500,
								}
								return err
							}
							envelope.RespCh <- ClientResponse{
								Success: true,
								Data:    stateMachineResponse,
							}
							return nil
						})
					}
				}
				if failed {
					envelope.RespCh <- ClientResponse{
						Success:      false,
						ErrorMessage: "Internal server error",
						ErrorCode:    500,
					}
				}
			case <- abortSignalChannel:
				return
			}
		}
	}()

	

	// separate goroutine to handle requests from other nodes
	go func () {
		wg.Add(1)
		defer func(){
			wg.Done()
		}()
		for {
			select {
			case req := <- raftNode.AppendEntriesCh:
				if req.Req.Term < leaderState.term {
					req.RespCh <- AppendEntriesResponse{
						Term: leaderState.term,
						Success: false,
					}
					continue
				}
				if req.Req.Term == leaderState.term {
					// split brain detected 
					// should not be possible but just for sanity check
					leaderState.logger.Error("split brain detected, rejecting append entries request", "req", req)
					req.RespCh <- AppendEntriesResponse{
						Term: leaderState.term,
						Success: false,
					}
					select {
					case abortSignalChannel <- struct{}{}:
					default:
					}
					return
				}
				// new term detected
				// let the next node handle the request or drop it if the buffer is full
				select {
				case raftNode.AppendEntriesCh <- req:
				default:
					req.RespCh <- AppendEntriesResponse{
						Term:    leaderState.term,
						Success: false,
					}
				}
				leaderState.newTermDetectionChannel <- newTermMessage{
					Term: req.Req.Term,
					LeaderId: req.Req.LeaderId,
				}
				return
			case req := <- raftNode.RequestVoteCh:
				if req.Req.Term <= leaderState.term {
					req.RespCh <- RequestVoteResponse{
						Term: leaderState.term,
						VoteGranted: false,
					}
					continue
				}
				// new term detected, step down
				// let the next node handle the request or drop it if the buffer is full
				select {
				case raftNode.RequestVoteCh <- req:
				default:
					req.RespCh <- RequestVoteResponse{
						Term: leaderState.term,
						VoteGranted: false,
					}
				}
				leaderState.newTermDetectionChannel <- newTermMessage{
					Term: req.Req.Term,
				}
				return
			case <- abortSignalChannel:
				return
			}
		}
	}()

	for {
		select {
		case newTermMsg := <-leaderState.newTermDetectionChannel:
			leaderState.logger.Info("New term detected, transitioning to follower", "new_term", newTermMsg.Term)
			// set new leader and the term and change voted for to 0
			if err := raftNode.Store.SetState(newTermMsg.Term, 0); err != nil {
				leaderState.logger.Error("error updating term on new term detection", "error", err)
				return &AbortState{}, err
			}
			raftNode.SetLeaderId(newTermMsg.LeaderId)
			return &FollowerState{}, nil

		case <-leaderState.checkCommitUpdateChannel:
			leaderState.logger.Debug("Checking for commit index update")
			raftNode.Membership.RwMu.RLock()

			newNodesMatches := make([]LogIndex, 0, len(raftNode.Membership.Members))
			oldNodesMatches := make([]LogIndex, 0, len(raftNode.Membership.Members))
			raftNode.Membership.forEachNode(func(nodeId NodeId, _ NodeAddress) {
				appendToNew := true
				appendToOld := true
				if nodeId == raftNode.Membership.ChangeNode {
					if raftNode.Membership.IsNodeRemoval {
						appendToNew = false
					} else {
						appendToOld = false
					}
				}
				if appendToNew {
					newNodesMatches = append(newNodesMatches, leaderState.matchIndex[nodeId])
				}
				if appendToOld {
					oldNodesMatches = append(oldNodesMatches, leaderState.matchIndex[nodeId])
				}
			})
			raftNode.Membership.RwMu.RUnlock()

			slices.Sort(oldNodesMatches)
			slices.Sort(newNodesMatches)

			toCommit := min(getMid(oldNodesMatches), getMid(newNodesMatches))

			leaderState.logger.Debug("Calculated commit index", "to_commit", toCommit)

			if toCommit >= max(raftNode.GetLastCommittedIndex()+1, startIndex) {
				leaderState.logger.Info("Updating commit index", "new_commit_index", toCommit)
				raftNode.SetLastCommittedIndex(toCommit)
			}

		// whatever being recieved is the final call, either to add or remove follower from the communication
		case membershipChange := <-membershipChangesChannel:
			if membershipChange.IsNodeRemoval {
				leaderState.logger.Info("Node removal detected, updating communication channels", "removed_node_id", membershipChange.NodeId)
				leaderState.followerContextsCancel[membershipChange.NodeId]()
			} else {
				leaderState.logger.Info("Node addition detected, updating communication channels", "added_node_id", membershipChange.NodeId, "added_node_address", membershipChange.NodeAddress)
				// new follower added, start the communication with the follower immediately
				followerContext, cancelFunc := context.WithCancel(leaderContext)
				leaderState.followerContextsCancel[membershipChange.NodeId] = cancelFunc
				wg.Add(1)
				go func() {
					defer wg.Done()
					leaderState.handleFollowerCommunication(raftNode, membershipChange.NodeId, membershipChange.NodeAddress, followerContext)
				}()
			}

		case <-raftNode.ShutdownCh:
			leaderState.logger.Info("Received shutdown signal, shutting down...")
			select {
			case abortSignalChannel <- struct{}{}:
			default:
			}
			return &AbortState{}, nil

		case <-abortSignalChannel:
			return &AbortState{}, nil
		}
	}
}

func getMid(arr []LogIndex) LogIndex {
	return arr[len(arr)/2]
}

func (leaderState *LeaderState) handleFollowerCommunication(raftNode *RaftNode, followerId NodeId, followerAddress NodeAddress, followerContext context.Context) error {

	leaderState.logger.Debug("starting follower communication", "follower_id", followerId)

	heartbeatTimeout := raftNode.Config.ElectionTimeoutMin / 2
	timer := time.NewTimer(heartbeatTimeout)

	initiateCommunicationChannel := make(chan struct{}, 1)
	initiateCommunicationChannel <- struct{}{}

	defer func() {
		timer.Stop()
		leaderState.logger.Debug("follower communication completed", "follower_id", followerId)
	}()

	for {
		select {
		case <-initiateCommunicationChannel:
			lastLogIndex, err := raftNode.Store.GetLastLogIndex()
			if err != nil {
				leaderState.logger.Error("error getting last log index", "error", err)
				initiateCommunicationChannel <- struct{}{}
				return err
			}
			lastIndexToSend := min(leaderState.nextIndex[followerId]+LogIndex(raftNode.Config.MaxEntriesPerAppend-1), lastLogIndex)
			entriesToSend, err := raftNode.Store.GetLogEntries(leaderState.nextIndex[followerId], lastIndexToSend)
			prevLogTerm, err := raftNode.Store.GetLogTerm(leaderState.nextIndex[followerId] - 1)
			if err != nil {
				leaderState.logger.Error("error getting previous log term", "error", err)
				return err
			}
			appendEntriesResponse, err := raftNode.Transport.SendAppendEntries(followerContext, followerAddress,
				AppendEntriesRequest{
					Term:         leaderState.term,
					LeaderId:     leaderState.leaderId,
					PrevLogIndex: leaderState.nextIndex[followerId] - 1,
					PrevLogTerm:  prevLogTerm,
					Entries:      entriesToSend,
					LeaderCommit: raftNode.GetLastCommittedIndex(),
				})

			if err != nil {
				leaderState.logger.Error("error sending append entries", "error", err)
				initiateCommunicationChannel <- struct{}{}
				continue
			}

			// new leader detected
			if appendEntriesResponse.Term > leaderState.term {
				leaderState.logger.Debug("new term detected", "new_term", "new_leader_term", strconv.FormatInt(int64(appendEntriesResponse.Term), 10), "communicating message to the leader process")
				select {
				case leaderState.newTermDetectionChannel <- newTermMessage{
					Term:     appendEntriesResponse.Term,
					LeaderId: 0,
				}:
				default:
					// already a new term message in the channel ignore
					leaderState.logger.Debug("push to new term detection channel failed, already a new term message in the channel")
				}
				return nil
			}

			if appendEntriesResponse.Success {
				leaderState.logger.Info("Append entries successful for follower", "follower_id", followerId, "next_index", leaderState.nextIndex[followerId], "prev_log_index", leaderState.nextIndex[followerId]-1, "prev_log_term", prevLogTerm)
				leaderState.matchIndex[followerId] = leaderState.nextIndex[followerId] - 1 + LogIndex(len(entriesToSend))
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
						leaderState.nextIndex[followerId] = appendEntriesResponse.ConflictIndex
					} else {
						// conflict term matched, skip to the next index
						leaderState.logger.Info("Conflict term matched, skipping to the next index", "follower_id", followerId, "conflict_term", appendEntriesResponse.ConflictTerm, "conflict_index", appendEntriesResponse.ConflictIndex)
						leaderState.nextIndex[followerId] = appendEntriesResponse.ConflictIndex + 1
						leaderState.matchIndex[followerId] = appendEntriesResponse.ConflictIndex
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
			leaderState.logger.Debug("Received shutdown signal from leader", "follower_id", followerId)
			ResetTimer(timer, heartbeatTimeout)
			return nil
		}
	}

}
