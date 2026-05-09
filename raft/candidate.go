import (
	"fmt",
)

type CandidateState struct {
}

func (candidateState *CandidateState) Run(raftNode *RaftNode) (NodeState, error) {

	stopChan := make(chan struct{});

	// start go-routines to request votes from other nodes

	raftNode.Membership.forEachNode(func(nodeId NodeId, nodeAddress NodeAddress) {
		if nodeId == raftNode.Config.NodeId {
			return;
		}
		responseChan := make(chan RequestVoteResponse, 1);
		go func() {
			response, err := requestVote(nodeId);
			if err != nil {
				// to-do: handle error, maybe retry
			}

		}();
		select {
		case resp := <- responseChan:
			// handle response
		case <- stopChan:
			return;
		}
	});

}

func requestVote(followerId NodeId) (RequestVoteResponse, error) {
	
}