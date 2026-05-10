package raft;
import (
	"time";
	"math/rand/v2";
	"sync";
)

type FollowerState struct {
}

func resetTimer(timer *time.Timer, heartbeatTimeout time.Duration){
	if(!timer.Stop()){
		// drain the expiry channel if already expired
		select {
		case <- timer.C:
		default:
		}
	}
	timer.Reset(heartbeatTimeout);
}


func (followerState* FollowerState) Run(raftNode *RaftNode) (NodeState, error) {
	wg := sync.WaitGroup{};
	heartbeatTimeout := RandomDuration(raftNode.Config.ElectionTimeoutMin, raftNode.Config.ElectionTimeoutMax);
	timer := time.NewTimer(heartbeatTimeout);

	localAppendEntriesRequestChannel := make(chan AppendEntriesEnvelope, 10);
	// channel to stop the go-routines
	stopChan := make(chan struct{}, 10);

	resetTimerChannel := make (chan bool, 10);

	wg.Add(1);
	go func() {
		defer wg.Done();
		for {
			select {
			case x := <- raftNode.ClientRequestCh:
				x.RespCh <- ClientResponse{
					Success: false,
					LeaderId: raftNode.GetLeaderId(),
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
			case <-raftNode.ShutdownCh:
				// shutdown signal received from top level
				close(stopChan);
				return;
			case <- timer.C:
				// timer expired, shutdown the go-routines
				close(stopChan);
				return;
			case x := <- raftNode.AppendEntriesCh:
				resetTimer(timer, heartbeatTimeout);
				localAppendEntriesRequestChannel <- x;
			case <- resetTimerChannel:
				resetTimer(timer, heartbeatTimeout);
			case <- stopChan:
				return;
			}
		}
	}();

	wg.Add(1);
	go func() {
		defer wg.Done();
		for {
			select {
			case x := <- localAppendEntriesRequestChannel:
				term := raftNode.Store.GetCurrentTerm();
				// old leader check
				if x.Req.Term < term {
					x.RespCh <- AppendEntriesResponse{
						Term: term,
						Success: false,
					}
					continue;
				} else if ((x.Req.Term > term) || (raftNode.GetLeaderId() == 0)) {
					term = x.Req.Term;
					raftNode.Store.SetCurrentTerm(x.Req.Term);
					raftNode.Store.SetVotedFor(x.Req.LeaderId);
					raftNode.SetLeaderId(x.Req.LeaderId);
				}
			
				log, err := raftNode.Store.GetLogEntry(x.Req.PrevLogIndex);

				if err != nil {
					// PrevLogIndex doesn't exist
					if _, ok := err.(LogIndexOutOfBoundsError); ok {
						lastLogIndex := raftNode.Store.GetLastLogIndex();
						lastLogTerm := raftNode.Store.GetLastLogTerm();
						x.RespCh <- AppendEntriesResponse{
							Term: term,
							Success: false,
							ConflictIndex: lastLogIndex,
							ConflictTerm: lastLogTerm,
						}
						continue;
					}
					// To-do: error handling for other errors
				}

				// detected conflict in log
				if(log.Term != x.Req.PrevLogTerm){
					// get the first log index of the conflicting term
					firstLogIndex := raftNode.Store.GetFirstLogIndex(x.Req.PrevLogTerm);

					x.RespCh <- AppendEntriesResponse{
						Term: term,
						Success: false,
						ConflictIndex: firstLogIndex,
						ConflictTerm: x.Req.PrevLogTerm,
					}

					continue;
				}

				lastStoreLogIndex := raftNode.Store.GetLastLogIndex();
				// append entries
				entriesToPatch, firstUnmatchedIndex := getEntriesToPatch(x.Req.Entries, x.Req.PrevLogIndex, lastStoreLogIndex, raftNode.Store);

				lastEntriesIndex := firstUnmatchedIndex + LogIndex(len(entriesToPatch)) - 1;

				// patch the local log and truncate if there is a conflict
				if(len(x.Req.Entries) > 0){
					diskwg := sync.WaitGroup{};
					diskwg.Add(1);
					go func() {
						defer diskwg.Done();
						if((lastEntriesIndex < lastStoreLogIndex) && (firstUnmatchedIndex != lastEntriesIndex + 1)){
							raftNode.Store.TruncateFrom(lastEntriesIndex + 1);
						}
					}();

					raftNode.Store.PatchEntries(entriesToPatch);
					
					diskwg.Wait();
				}

				commitIndex := min(lastEntriesIndex, x.Req.LeaderCommit);
				raftNode.Store.SetLastCommittedIndex(commitIndex);
				// maybe trigger a communication to the store saying that the commit index got updated

				// send the response to leader that the prevLogIndex and the prevLogTerm are valid and append asynchronously
				x.RespCh <- AppendEntriesResponse{
					Term: term,
					Success: true,
				}

			case x := <- raftNode.RequestVoteCh:

				term := raftNode.Store.GetCurrentTerm();

				if(x.Req.Term < term){
					x.RespCh <- RequestVoteResponse{
						Term: term,
						VoteGranted: false,
					}
					continue;
				}
				if(x.Req.Term > term){
					term = x.Req.Term;
					raftNode.Store.SetCurrentTerm(x.Req.Term);
					raftNode.Store.SetVotedFor(0);
					raftNode.SetLeaderId(0);
				}

				if(raftNode.Store.GetVotedFor() == 0){
					if (x.Req.LastLogTerm > raftNode.Store.GetLastLogTerm()) || (x.Req.LastLogTerm == raftNode.Store.GetLastLogTerm() && x.Req.LastLogIndex >= raftNode.Store.GetLastLogIndex()){
						resetTimerChannel <- true;
						raftNode.Store.SetVotedFor(x.Req.CandidateId);
						x.RespCh <- RequestVoteResponse{
							Term: term,
							VoteGranted: true,
						}
						continue;
					}
				}

				x.RespCh <- RequestVoteResponse{
					Term: term,
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
		if(!ok){
			return AbortState{}, nil;
		}
	default:
	}

	// if we are here, then the go-routines are returned because of election timeout
	return CandidateState{}, nil;

}

// implement this using binary search
// returns the entries to patch along with the index of the first unmatched entry
func getEntriesToPatch(logEntries []LogEntry, prevLogIndex LogIndex, lastStoreLogIndex LogIndex, store Store) ([]LogEntry, LogIndex){
	if(len(logEntries) == 0){
		return nil, prevLogIndex+1;
	}

	// in most of the cases, the log entries sent by leader are continous so check that case
	if(lastStoreLogIndex == prevLogIndex){
		return logEntries, prevLogIndex+1;
	}

	maxBSIndex := LogIndex(min(int(len(logEntries) + int(prevLogIndex)), int(lastStoreLogIndex)));
	minBSIndex := LogIndex(prevLogIndex + 1);

	for maxBSIndex >= minBSIndex {
		midIndex := (maxBSIndex + minBSIndex) / 2;
		midLogTerm, err := store.GetLogTerm(midIndex);

		var unmatched bool = false;

		// entry doesn't exist in the local log
		if err != nil {
			if _, ok := err.(LogIndexOutOfBoundsError); ok {
				unmatched = true;
			}
			// To-do: error handling for other errors
		} else if (midLogTerm != logEntries[midIndex-(prevLogIndex+1)].Term) {
			// entry exists but there is a conflict
			unmatched = true;
		}

		if(unmatched) {
			maxBSIndex = midIndex - 1;
		}
		else{
			minBSIndex = midIndex + 1;
		}
	}
	unmatchedIndex := minBSIndex;
	return logEntries[(unmatchedIndex-(prevLogIndex+1)):], unmatchedIndex;
}