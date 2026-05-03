package raft;
import "time";

type FollowerState struct {
	LastHeartbeat time.Time;
	HeartbeatTimeoutChannel chan time.Time;
	HeartbeatTimeout chan time.Duration;
}

func (followerState* FollowerState) Run(raftNode *RaftNode) error {

}