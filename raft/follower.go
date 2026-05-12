package raft;

import (
	"log/slog"
	"sync"
	"time"
)

type FollowerState struct {
}

func resetTimer(timer *time.Timer, heartbeatTimeout time.Duration) {
	if !timer.Stop() {
		// drain the expiry channel if already expired
		select {
		case <- timer.C:
		default:
		}
	}
	timer.Reset(heartbeatTimeout);
}


func (followerState *FollowerState) Run(raftNode *RaftNode) (NodeState, error) {
	logger := raftNode.Config.Logger.With(
		"state", "follower",
		"node_id", raftNode.Config.NodeId,
	);

	wg := sync.WaitGroup{};
	heartbeatTimeout := RandomDuration(raftNode.Config.ElectionTimeoutMin, raftNode.Config.ElectionTimeoutMax);
	timer := time.NewTimer(heartbeatTimeout);

	logger.Info("entering follower state", "heartbeat_timeout", heartbeatTimeout);

	// channel to stop the go-routines
	stopChan := make(chan struct{}, 10);
	resetTimerChan := make(chan struct{}, 10);

	wg.Add(1);
	go func() {
		defer wg.Done();
		for {
			select {
			case x := <- raftNode.ClientRequestCh:
				leaderId := raftNode.GetLeaderId();
				logger.Debug("redirecting client request to leader", "leader_id", leaderId);
				x.RespCh <- ClientResponse{
					Success:  false,
					LeaderId: leaderId,
				}
			case <- stopChan:
				return;
			}
		}
	}();

	wg.Add(1);
	// the ownership of timer is handled by this go-routine
	go func() {
		defer wg.Done();
		for {
			select {
			case <- resetTimerChan:
				resetTimer(timer, heartbeatTimeout);
			case <-raftNode.ShutdownCh:
				// shutdown signal received from top level
				logger.Info("shutdown signal received");
				close(stopChan);
				return;
			case <- timer.C:
				// timer expired, shutdown the go-routines
				logger.Info("heartbeat timeout expired; transitioning to candidate");
				close(stopChan);
				return;
			case <- stopChan:
				return;
			}
		}
	}();

	var errReturned error;
	var nextState NodeState;

	wg.Add(1);
	go func() {
		defer wg.Done();
	loop:
		for {
			select {
			case x := <- raftNode.AppendEntriesCh:
				term := raftNode.Store.GetCurrentTerm();
				peerLogger := logger.With(
					"peer_id", x.Req.LeaderId,
					"peer_term", x.Req.Term,
					"current_term", term,
					"prev_log_index", x.Req.PrevLogIndex,
					"prev_log_term", x.Req.PrevLogTerm,
				);

				// old leader check
				if x.Req.Term < term {
					peerLogger.Warn("rejecting AppendEntries from stale leader");
					x.RespCh <- AppendEntriesResponse{
						Term:    term,
						Success: false,
					}
					continue;
				}
				resetTimerChan <- struct{}{};
				if (x.Req.Term > term) || (raftNode.GetLeaderId() == 0) {
					if x.Req.Term > term {
						peerLogger.Info("observed higher term; adopting leader");
						if err := raftNode.Store.SetCurrentTerm(x.Req.Term); err != nil {
							peerLogger.Error("store write failed; continuing", "op", "SetCurrentTerm", "term", x.Req.Term, "error", err);
							x.RespCh <- AppendEntriesResponse{
								Term:    term,
								Success: false,
							}
							// not fatal error, ignore the request and continue
							continue;
						}
						if err := raftNode.Store.SetVotedFor(0); err != nil {
							peerLogger.Error("store write failed; continuing", "op", "SetVotedFor", "error", err);
							// fatal error, the term got incremented but the votedFor is not set to 0
							nextState = &AbortState{};
							errReturned = err;
							break loop;
						}
					} else {
						peerLogger.Info("first contact with leader at current term");
					}
					term = x.Req.Term;
					raftNode.SetLeaderId(x.Req.LeaderId);
				}

				log, err := raftNode.Store.GetLogEntry(x.Req.PrevLogIndex);
				if err != nil {
					if _, ok := err.(LogIndexOutOfBoundsError); ok {
						lastLogIndex, idxErr := raftNode.Store.GetLastLogIndex();
						lastLogTerm, termErr := raftNode.Store.GetLastLogTerm();
						if idxErr != nil || termErr != nil {
							peerLogger.Error("store read failed; rejecting AppendEntries without conflict hint",
								"op", "GetLastLogIndex/Term", "index_error", idxErr, "term_error", termErr);
							x.RespCh <- AppendEntriesResponse{
								Term:    term,
								Success: false,
							}
							continue;
						}
						peerLogger.Debug("PrevLogIndex past end of local log; replying with conflict hint",
							"last_log_index", lastLogIndex, "last_log_term", lastLogTerm);
						x.RespCh <- AppendEntriesResponse{
							Term:          term,
							Success:       false,
							ConflictIndex: lastLogIndex,
							ConflictTerm:  lastLogTerm,
						}
						continue;
					}
					// not a fatal error, ignore the request and continue
					// Unknown store error — log and reject this RPC; do not panic on log.Term below.
					peerLogger.Error("store read failed; rejecting AppendEntries",
						"op", "GetLogEntry", "index", x.Req.PrevLogIndex, "error", err);
					x.RespCh <- AppendEntriesResponse{
						Term:    term,
						Success: false,
					}
					continue;
				}

				// detected conflict in log
				if log.Term != x.Req.PrevLogTerm {
					conflictIndex, err := raftNode.Store.GetFirstLogIndex(log.Term);
					if err != nil {
						peerLogger.Error("store read failed; rejecting AppendEntries without conflict hint",
							"op", "GetFirstLogIndex", "term", log.Term, "error", err);
						x.RespCh <- AppendEntriesResponse{
							Term:    term,
							Success: false,
						}
						continue;
					}
					peerLogger.Debug("PrevLogTerm mismatch; replying with conflict hint",
						"local_term_at_prev_index", log.Term,
						"conflict_index", conflictIndex);
					x.RespCh <- AppendEntriesResponse{
						Term:          term,
						Success:       false,
						ConflictIndex: conflictIndex,
						ConflictTerm:  log.Term,
					}
					continue;
				}

				lastStoreLogIndex, err := raftNode.Store.GetLastLogIndex();
				if err != nil {
					peerLogger.Error("store read failed; rejecting AppendEntries",
						"op", "GetLastLogIndex", "error", err);
					x.RespCh <- AppendEntriesResponse{
						Term:    term,
						Success: false,
					}
					continue;
				}
				entriesToPatch, firstUnmatchedIndex := getEntriesToPatch(x.Req.Entries, x.Req.PrevLogIndex, lastStoreLogIndex, raftNode.Store, peerLogger);
				lastEntriesIndex := firstUnmatchedIndex + LogIndex(len(entriesToPatch)) - 1;

				if len(x.Req.Entries) > 0 {
					// Truncate (if there's a conflicting tail) then patch. Sequential is fine on a single disk
					// and gives a clean crash-recovery story.
					if (lastEntriesIndex < lastStoreLogIndex) && (firstUnmatchedIndex != lastEntriesIndex + 1) {
						if err := raftNode.Store.TruncateFrom(lastEntriesIndex + 1); err != nil {
							peerLogger.Error("store write failed; continuing", "op", "TruncateFrom", "from", lastEntriesIndex+1, "error", err);
						}
					}
					if err := raftNode.Store.PatchEntries(entriesToPatch); err != nil {
						peerLogger.Error("store write failed; rejecting AppendEntries",
							"op", "PatchEntries", "first_unmatched_index", firstUnmatchedIndex,
							"num_entries", len(entriesToPatch), "error", err);
						x.RespCh <- AppendEntriesResponse{
							Term:    term,
							Success: false,
						}
						// not a fatal error
						continue;
					}
				}

				commitIndex := min(lastEntriesIndex, x.Req.LeaderCommit);
				raftNode.SetLastCommittedIndex(commitIndex);
				peerLogger.Debug("AppendEntries applied",
					"first_unmatched_index", firstUnmatchedIndex,
					"last_entries_index", lastEntriesIndex,
					"commit_index", commitIndex);

				x.RespCh <- AppendEntriesResponse{
					Term:    term,
					Success: true,
				}

			case x := <- raftNode.RequestVoteCh:
				term := raftNode.Store.GetCurrentTerm();
				voteLogger := logger.With(
					"candidate_id", x.Req.CandidateId,
					"peer_term", x.Req.Term,
					"current_term", term,
				);

				if x.Req.Term < term {
					voteLogger.Debug("rejecting RequestVote at stale term");
					x.RespCh <- RequestVoteResponse{
						Term:        term,
						VoteGranted: false,
					}
					continue;
				}
				if x.Req.Term > term {
					voteLogger.Info("observed higher term in RequestVote; clearing votedFor");
					if err := raftNode.Store.SetCurrentTerm(x.Req.Term); err != nil {
						voteLogger.Error("store write failed; rejecting vote",
							"op", "SetCurrentTerm", "term", x.Req.Term, "error", err);
						// not fatal: term not advanced in store, reject and continue
						x.RespCh <- RequestVoteResponse{
							Term:        term,
							VoteGranted: false,
						}
						continue;
					}
					if err := raftNode.Store.SetVotedFor(0); err != nil {
						voteLogger.Error("store write failed; aborting",
							"op", "SetVotedFor", "error", err);
						// fatal: term got incremented but votedFor not cleared — invariant violation
						nextState = &AbortState{};
						errReturned = err;
						break loop;
					}
					term = x.Req.Term;
					raftNode.SetLeaderId(0);
				}

				votedFor := raftNode.Store.GetVotedFor();
				if votedFor == 0 {
					lastLogTerm, termErr := raftNode.Store.GetLastLogTerm();
					lastLogIndex, idxErr := raftNode.Store.GetLastLogIndex();
					if termErr != nil || idxErr != nil {
						voteLogger.Error("store read failed; cannot compare logs, rejecting vote",
							"op", "GetLastLogIndex/Term", "index_error", idxErr, "term_error", termErr);
						x.RespCh <- RequestVoteResponse{
							Term:        term,
							VoteGranted: false,
						}
						continue;
					}
					logUpToDate := (x.Req.LastLogTerm > lastLogTerm) ||
						(x.Req.LastLogTerm == lastLogTerm && x.Req.LastLogIndex >= lastLogIndex);
					if logUpToDate {
						if err := raftNode.Store.SetVotedFor(x.Req.CandidateId); err != nil {
							voteLogger.Error("store write failed; rejecting vote",
								"op", "SetVotedFor", "candidate_id", x.Req.CandidateId, "error", err);
							x.RespCh <- RequestVoteResponse{
								Term:        term,
								VoteGranted: false,
							}
							continue;
						}
						resetTimerChan <- struct{}{};
						voteLogger.Info("vote granted");
						x.RespCh <- RequestVoteResponse{
							Term:        term,
							VoteGranted: true,
						}
						continue;
					}
					voteLogger.Debug("vote rejected: candidate log not up-to-date",
						"local_last_log_term", lastLogTerm,
						"local_last_log_index", lastLogIndex,
						"peer_last_log_term", x.Req.LastLogTerm,
						"peer_last_log_index", x.Req.LastLogIndex);
				} else {
					voteLogger.Debug("vote rejected: already voted this term", "voted_for", votedFor);
				}

				x.RespCh <- RequestVoteResponse{
					Term:        term,
					VoteGranted: false,
				}

			case <- stopChan:
				return;
			}
		}
	}();

	wg.Wait();

	// if we are here and the shutdown channel is closed, return the abort state
	select {
	case _, ok := <- raftNode.ShutdownCh:
		if !ok {
			logger.Info("exiting follower state for shutdown");
			return &AbortState{}, nil;
		}
	default:
	}

	if nextState != nil {
		return nextState, errReturned;
	}

	logger.Info("exiting follower state for new election");
	return &CandidateState{}, nil;
}

// implement this using binary search
// returns the entries to patch along with the index of the first unmatched entry
func getEntriesToPatch(logEntries []LogEntry, prevLogIndex LogIndex, lastStoreLogIndex LogIndex, store Store, logger *slog.Logger) ([]LogEntry, LogIndex) {
	if len(logEntries) == 0 {
		return nil, prevLogIndex + 1;
	}

	// in most of the cases, the log entries sent by leader are continous so check that case
	if lastStoreLogIndex == prevLogIndex {
		return logEntries, prevLogIndex + 1;
	}

	maxBSIndex := LogIndex(min(int(prevLogIndex)+len(logEntries), int(lastStoreLogIndex)));
	minBSIndex := prevLogIndex + 1;

	for maxBSIndex >= minBSIndex {
		midIndex := (maxBSIndex + minBSIndex) / 2;
		midLogTerm, err := store.GetLogTerm(midIndex);

		unmatched := false;

		if err != nil {
			if _, ok := err.(LogIndexOutOfBoundsError); ok {
				unmatched = true;
			} else {
				// Unknown store error — treat conservatively as unmatched so we re-fetch this range.
				logger.Error("store read failed; treating as unmatched",
					"op", "GetLogTerm", "index", midIndex, "error", err);
				unmatched = true;
			}
		} else if midLogTerm != logEntries[midIndex-(prevLogIndex+1)].Term {
			unmatched = true;
		}

		if unmatched {
			maxBSIndex = midIndex - 1;
		} else {
			minBSIndex = midIndex + 1;
		}
	}
	unmatchedIndex := minBSIndex;
	return logEntries[(unmatchedIndex - (prevLogIndex + 1)):], unmatchedIndex;
}
