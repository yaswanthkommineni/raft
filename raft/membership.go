package raft;
// This file contains the implementation of cluster membership management.

// temporary and rebuilt on node restart
type Membership struct {
	// Members is the set of node IDs that are currently part of the cluster.
	Members map[NodeId]NodeAddress{};
	
	ChangeNode NodeId;
	IsNodeRemoval bool;
}

// write the membership apply logic here
func (membership* Membership) apply(logEntry* LogEntry) error{

}