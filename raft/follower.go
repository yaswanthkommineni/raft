package raft;
import {
	"time";
	"math/rand/v2";
	"sync";
}

type FollowerState struct {
}

func (followerState* FollowerState) Run(raftNode *RaftNode) error {
	wg := sync.WaitGroup{};
	wg.Add(5);
	heartbeatTimeout := raftNode.Config.ElectionTimeoutMin + time.Duration(rand.Intn(int(raftNode.Config.ElectionTimeoutMax - raftNode.Config.ElectionTimeoutMin)));
	timer := time.NewTimer(heartbeatTimeout);

	localAppendEntriesRequestChannel := make(chan AppendEntriesEnvelope, 10);
	leaderId := NodeId(0);
	// channel to stop the go-routines
	stopChan := make(chan struct{});

	go func() {
		defer wg.Done();
		for {
			select {
			case x := <- raftNode.ClientRequestCh:
				response := ClientResponse{
					Success: false,
					LeaderId: leaderId
				}
			}
			case <- stopChan:
				return;
		}
	}();

	go func() {
		defer wg.Done();
		for {
			select {
			case x := <- raftNode.AppendEntriesCh:
				if(!timer.Stop()){
					// drain the expiry channel if already expired
					select {
					case <- timer.C:
					default:
					}
				}
				timer.Reset(heartbeatTimeout);
				localAppendEntriesRequestChannel <- x;
			case <- stopChan:
				return;
			}
		}
	}();

	go func() {
		defer wg.Done();
		for {
			select {
			case <- timer.C:
				// timer expired, shutdown the go-routines
				close(stopChan);
				return;
			case <- stopChan:
				return;
			}
		}
	}();	

	go func() {
		defer wg.Done();
		for {
			select {
			case <-raftNode.ShutdownCh:
				// shutdown signal received from top level
				close(stopChan);
				return;
			case <- stopChan:
				return;
			}
		}
	}();

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
				}
				else if x.Req.Term > term {
					raftNode.Store.setCurrentTerm(x.Req.Term);
					raftNode.Store.setVotedFor(x.Req.LeaderId);
					leaderId = x.Req.LeaderId;
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
				}

				// detected conflict in log
				if(log.Term != x.Req.PrevLogTerm){
					// get the first log index of the conflicting term
					firstLogIndex := raftNode.Store.GetFirstLogIndex(x.Req.PrevLogTerm);

					raftNode.Store.TruncateFrom();

					x.RespCh <- AppendEntriesResponse{
						Term: term,
						Success: false,
						ConflictIndex: firstLogIndex,
						ConflictTerm: x.Req.PrevLogTerm,
					}
					continue;
				}

				// append entries
				entriesToPatch := getEntriesToPatch(x.Req.Entries, x.Req.PrevLogIndex, raftNode.Store);
				raftNode.Store.PatchEntries(entriesToPatch);

				entriesLen := len(x.Req.Entries)
				if(entriesLen > 0){

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
func getEntriesToPatch(logEntries[] LogEntry, prevLogIndex LogIndex, Store store) []LogEntry{
	lastStoreLogIndex := Store.GetLastLogIndex();
	lastStoreLogTerm := Store.GetLastLogTerm();

}