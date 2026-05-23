package raft

import (
	"errors"
)

// Membership is the in-memory cluster configuration. It is not persisted
// directly; on restart it is rebuilt by replaying the log via StoreInterpreter.
//
// Concurrency invariant:
//   - Exactly one goroutine ever mutates Membership state (the goroutine
//     that drives Store writes through StoreInterpreter — currently the
//     follower's AppendEntries handler, later the leader's main loop).
//   - Readers (e.g. candidate.incrementAndCheckVotes, forEachNode) do not
//     run concurrently with the writer today, because at most one node
//     state is active at a time and that state owns the writer goroutine.
//   - TODO(leader): once leader's per-peer replication goroutines start
//     reading Membership concurrently with the leader's own writes, this
//     invariant is no longer enough. At that point switch to either a
//     sync.RWMutex on Membership or an atomic.Pointer[Membership] on
//     RaftNode with snapshot-and-swap.
//
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
	ChangeNode     NodeId
	IsNodeRemoval  bool
	ChangesHistory Stack[MembershipChange]
	subscribers []chan MembershipChange
}

// cloneState returns a Membership that shares no mutable state with the
// receiver: Members and ChangesHistory are deep-copied. subscribers is
// intentionally NOT copied — the clone exists only to stage apply/revert
// without affecting the real Membership, so its notifySubscribers must
// be a no-op (see TODO on subscribers).
func (m *Membership) cloneState() *Membership {
	membersCopy := make(map[NodeId]NodeAddress, len(m.Members))
	for k, v := range m.Members {
		membersCopy[k] = v
	}
	historyCopy := make(Stack[MembershipChange], len(m.ChangesHistory))
	copy(historyCopy, m.ChangesHistory)
	return &Membership{
		Members:        membersCopy,
		ChangeNode:     m.ChangeNode,
		IsNodeRemoval:  m.IsNodeRemoval,
		ChangesHistory: historyCopy,
	}
}

// swapStateFrom adopts the mutable state of other into the receiver. The
// receiver keeps its subscribers. Caller must hold the single-writer
// invariant — see Membership concurrency notes.
func (m *Membership) swapStateFrom(other *Membership) {
	m.Members = other.Members
	m.ChangeNode = other.ChangeNode
	m.IsNodeRemoval = other.IsNodeRemoval
	m.ChangesHistory = other.ChangesHistory
}

func (membership *Membership) apply(logEntry *LogEntry) error {
	membershipChange, err := DecodeMembershipChange(logEntry.Data)
	if err != nil {
		return err
	}
	if membershipChange.Confirmation {
		if membershipChange.NodeId != membership.ChangeNode {
			if membership.ChangeNode == 0 {
				return errors.New("No prior change made to confirm")
			}
			return errors.New("Confirmation node mismatch")
		}
		if membership.IsNodeRemoval != membershipChange.IsNodeRemoval {
			return errors.New("Confirmation change type mismatch")
		}
		membership.ChangeNode = 0
		membership.IsNodeRemoval = false
		if membershipChange.IsNodeRemoval {
			delete(membership.Members, membershipChange.NodeId)
			membership.notifySubscribers(membershipChange)
		}
	} else {
		if membership.ChangeNode != 0 {
			return errors.New("Membership change in progress already")
		}
		membership.ChangeNode = membershipChange.NodeId
		membership.IsNodeRemoval = membershipChange.IsNodeRemoval
		if !membershipChange.IsNodeRemoval {
			membership.Members[membershipChange.NodeId] = membershipChange.NodeAddress
			membership.notifySubscribers(membershipChange)
		}
	}
	membership.ChangesHistory.Push(membershipChange)
	return nil
}

// reverts the last membership change
func (membership *Membership) revertMembershipChange() error {
	change, ok := membership.ChangesHistory.Pop()
	if !ok {
		return errors.New("No membership change to revert")
	}
	if change.Confirmation {
		if change.NodeId == 0 {
			return errors.New("Logical error: confirmation must carry a NodeId")
		}
		if change.IsNodeRemoval {
			membership.Members[change.NodeId] = change.NodeAddress
			membership.notifySubscribers(MembershipChange{
				NodeId:        change.NodeId,
				IsNodeRemoval: !change.IsNodeRemoval,
			})
		}
		membership.ChangeNode = change.NodeId
		membership.IsNodeRemoval = change.IsNodeRemoval
	} else {
		if change.NodeId == 0 {
			return errors.New("Logical error: initial change must carry a NodeId")
		}
		if change.IsNodeRemoval != membership.IsNodeRemoval || change.NodeId != membership.ChangeNode {
			return errors.New("Logical error: revert state mismatch")
		}
		if !change.IsNodeRemoval {
			delete(membership.Members, change.NodeId)
			membership.notifySubscribers(MembershipChange{
				NodeId:        change.NodeId,
				IsNodeRemoval: !change.IsNodeRemoval,
			})
		}
		membership.ChangeNode = 0
		membership.IsNodeRemoval = false
	}
	return nil
}

// reverts all the membership changes whose LogIndex is >= logIndex
func (membership *Membership) revertMembershipChangesTill(logIndex LogIndex) error {
	for {
		change, ok := membership.ChangesHistory.Peek()
		if !ok {
			break
		}
		if change.LogIndex < logIndex {
			break
		}
		if err := membership.revertMembershipChange(); err != nil {
			return err
		}
	}
	return nil
}

func (membership *Membership) forEachNode(callback func(nodeId NodeId, nodeAddress NodeAddress)) {
	for nodeId, nodeAddress := range membership.Members {
		callback(nodeId, nodeAddress)
	}
}

func (membership *Membership) notifySubscribers(change MembershipChange) {
	for _, subscriber := range membership.subscribers {
		select {
		case subscriber <- change:
		default:
			// ignore if the subscriber is not ready to receive
		}
	}
}

func (membership *Membership) subscribeToMembershipChanges(ch chan MembershipChange) {
	membership.subscribers = append(membership.subscribers, ch)
}
