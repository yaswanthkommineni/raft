package raft

// This file contains the implementation of cluster membership management.

// temporary and rebuilt on node restart
type Membership struct {
	// Members is the set of node IDs that are currently part of the cluster.
	// During a membership change in progress, Members contains the union of the
	// old and new configurations (i.e. the joint set). Quorum math in candidate
	// state relies on this: oldClusterSize/newClusterSize are derived from len(Members)
	// and adjusted by ±1 depending on whether the change is an add or a remove.
	Members map[NodeId]NodeAddress

	// only one node can be added or removed at a time
	// should be set to 0 if there are no changes in progress
	ChangeNode    NodeId
	IsNodeRemoval bool
}

// write the membership apply logic here
func (membership *Membership) apply(logEntry *LogEntry) error {

}

func (membership *Membership) forEachNode(callback func(nodeId NodeId, nodeAddress NodeAddress)) {
	for nodeId, nodeAddress := range membership.Members {
		callback(nodeId, nodeAddress)
	}
}
