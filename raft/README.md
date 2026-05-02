# raft

Core algorithm package. Defines the interfaces (`Transport`, `LogStore`, `StateMachine`) that consumers implement and wire in. Imports nothing operational — no network, no disk, no logger backend.
