package raft

type LeaderState struct {

}

func (leaderState *LeaderState) Run(raftNode *RaftNode) (NodeState, error) {
	nextIndex := make([]LogIndex, raftNode.Membership.Members)
}
