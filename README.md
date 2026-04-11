# raft

## Preface

This repository contains the implementation of Raft, inspired by the paper https://raft.github.io/raft.pdf

## Scope

1. Leader election
2. Log replication
3. Persistence
4. Crash recovery snapshots
5. Basic client dedup
6. Multithreaded transport/storage metrics
7. Logging
8. Fault-injection tests
9. Membership changes
10. Observability

Note: Persistence is decoupled from the Raft implementation. This enables reusability and extensibility of the algorithm by supporting multiple storage models.