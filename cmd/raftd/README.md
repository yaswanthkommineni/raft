# cmd/raftd

The Raft daemon binary. The only place that imports concrete implementations from `transport/`, `storage/`, and `statemachine/` and wires them into a `raft.Node`.
