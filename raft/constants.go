package raft

// MaxClusterSize caps cluster membership.
// Raft consensus latency degrades past 7 nodes — every commit waits
// on a majority quorum, and the marginal availability gain past 7 is dominated by the latency cost.
const MaxClusterSize = 20
