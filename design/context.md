# Project Context

This file is the persistent memory of the project. It captures **why** this project exists, **how** it is being built, and **where it currently stands**. Read this first before doing any work in this repo.

---

## Purpose

The owner of this project is building Raft from scratch with two goals, in order:

1. **Become a distributed systems and platform engineer with strong design instincts.** This is the primary goal. Every decision should be evaluated against "does this teach me good design?" not "does this ship faster?"
2. **Produce a production-quality Raft implementation** in Go that could realistically be used or extended by others.

Design quality is non-negotiable. Shortcuts that compromise the learning experience or the production-readiness of the design are not acceptable. Shortcuts that are clearly scoped technical debt (e.g., "in-memory storage now, disk later") are fine and expected.

### How the assistant should behave

- **Do not write code for the owner.** The owner writes the code. The assistant helps them think.
- **Do not over-explain.** When the owner asks a focused question, give a focused answer. Long structural lectures are unwelcome unless explicitly requested.
- **Push back on design decisions** when they would compromise learning or production quality. Be honest about trade-offs.
- **Respect the owner's existing design** in `design/` (LLD, communication, persistence, deployment, testability). Refer to those rather than re-inventing.

---

## Implementation Plan

### Architectural commitments (decided)

- **Raft core is a library**, not part of an application. The config-store / KV-store is one consumer of the library. This forces clean module boundaries.
- **Core-plus-adapters package layout**: one `raft` package owns the algorithm and defines the interfaces it needs (`Transport`, `LogStore`, `StateMachine`). Concrete implementations live in sibling packages and are injected at wiring time. The algorithm package never imports an implementation. This is the etcd / hashicorp pattern.
- **gRPC + protobuf** is the production wire format, with `.proto` in `proto/` and generated code in `gen/`. Protobuf types are isolated from the algorithm — a thin adapter in the gRPC transport package converts between wire types and domain types. The algorithm package never imports `pb`.
- **Two transport implementations**: an in-memory transport (goroutines + channels, used for tests) and a gRPC transport (used in production). Both satisfy the same `Transport` interface. The in-memory one is built first because deterministic, fast tests are non-negotiable for a Raft implementation.
- **Two storage implementations**: an in-memory `LogStore` first, a disk-backed one later. Both satisfy the same `LogStore` interface.
- **Persistence is decoupled from Raft.** This is already in `README.md` and is enforced via the `LogStore` and `StateMachine` interfaces.
- **NodeID-based addressing in the algorithm.** The algorithm only ever knows about `NodeID`s. Each transport implementation maintains its own peer address book (e.g., the gRPC transport has `map[NodeID]"host:port"`). Address resolution is a transport concern, not an algorithm concern.

### Build sequence (decided)

The chosen approach is **vertical slice + test harness in parallel**: get the smallest possible end-to-end Raft slice working, with a deterministic in-process transport from day one. Persistence on disk, snapshots, and membership changes come after the core algorithm is working in-memory.

Rough order:

1. Project scaffold (`go mod init`, directory tree, `Makefile`, `.gitignore`).
2. Domain types in `raft/types.go` — `Term`, `LogIndex`, `NodeID`, `LogEntry`, the four RPC request/response structs from `design/communication.md`.
3. Three core interfaces in `raft/interfaces.go` — `Transport`, `LogStore`, `StateMachine`. Contracts documented in comments, especially durability guarantees.
4. In-memory `LogStore` + in-process `Transport` with thorough unit tests.
5. `Follower` state — receive `AppendEntries`, respond. Single node test.
6. `Candidate` state — election, term increment on timeout. Multi-node test in one process.
7. `Leader` state — replication, commit index advancement. End-to-end `Put` working against an in-memory state machine.
8. gRPC transport as a second `Transport` implementation. Wire `cmd/raftd`.
9. Disk-backed `LogStore`.
10. Snapshots, membership changes, chaos testing — per the existing design docs.

### Target directory layout

```
raft/                  # algorithm + interfaces (one package)
storage/               # implementations of LogStore (memory now, disk later)
transport/             # implementations of Transport (inmem now, grpc later)
statemachine/          # the config-store application
cmd/raftd/             # wires everything; the only place that imports all impls
proto/  gen/           # protobuf source + generated code
test/                  # multi-node integration tests, chaos harness
design/                # design docs (already populated)
```

Granularity inside `storage/` and `transport/` (single file vs subdirectory per impl) is a taste call to be made when the first impl gets large enough to need splitting.

---

## Current State

### What is done

- **Design phase is substantially complete** in `design/`:
  - `communication.md` — internal RPCs (`RequestVote`, `AppendEntries`, `InstallSnapshot`) and external client APIs (`Put`, `Get`, `Delete`, `CAS`).
  - `persistence.md` — KV shape with versioning for CAS support.
  - `deployment.md` — container model, bootstrap vs join logic, membership management.
  - `testability.md` — unit / integration / regression test layers, chaos harness design (failure injection via special log entry).
  - `low-leve-design.java` — detailed LLD covering state machine interface, log interface, storage, node states (Leader / Follower / Candidate), RPC services, channel-based concurrency model.
- **Architectural commitments above are decided.**

### What is in progress

- **Build sequence step 1 (project scaffold)** — partially done:
  - `go.mod` created with module path `github.com/komminy/raft`, Go 1.22.
  - `.gitignore` created.
  - Directory tree created per the target layout (`raft/`, `transport/{inmem,grpc}/`, `storage/{memory,disk}/`, `statemachine/`, `cmd/raftd/`, `proto/`, `gen/`, `test/`), each with a one-line `README.md` describing its purpose.
  - `cmd/raftd/main.go` exists as an empty stub so `go build ./...` will succeed.
  - **Not yet done:** `Makefile`. Go toolchain not yet on PATH for the owner — `go build` not yet verified.

### What is not done

- **Build sequence steps 2–10 are all pending**, in order.
- **No algorithm code yet** — `raft/` is empty apart from its README. Domain types and interfaces (steps 2–3) are the owner's next task.

### Serious issues to address

These are concerns flagged during design review that need attention before or during implementation. They are not blockers for starting, but they will cause pain if not handled:

- **`LogEntry.jsonString` in the LLD.** The current LLD types the log payload as a JSON string. JSON costs ~5–10× the bytes of binary, can't represent `[]byte` cleanly, and re-parses on every apply. Recommendation: payload should be `[]byte` and the encoding chosen by the `StateMachine` consumer, so the algorithm is encoding-agnostic.
- **`Storage` class mixes concerns.** The LLD's `Storage` class bundles persistent fields (`term`, `votedFor`, `log`), non-persistent algorithm state (`lastCommitted`, `lastApplied`), and the state machine reference. These have different lifecycles, locking requirements, and test surfaces. Likely wants to be ~3 separate types with clear ownership.
- **"Thread mapper" in `LeaderState`.** The LLD's `Map<int, Thread> threadMapper` is a Java-ism. The Go idiom is one goroutine per peer that owns its own state and communicates via channels — no shared mutable map of goroutine handles.
- **`gatheredVotes` as atomic int.** In idiomatic Go, vote results should be sent down a channel to a single goroutine that owns the count. Atomics work but channels read more clearly and compose with timeouts.
- **Durability contract for `LogStore` is implicit.** The interface must explicitly state — in comments — that `Append` must not return until the entry is on stable storage, and `SaveState` likewise for term/votedFor. If this is left ambiguous, an implementer will get it wrong and Raft will lose data. This is the single most important contract in the whole interface set.
- **Snapshot support in `StateMachine` interface.** The naive `Apply(entry) result` is not enough. Snapshots need `Snapshot() io.Reader` and `Restore(io.Reader) error`. These can be added later but the interface design should leave room.

### What is good

- **Strong, deliberate design before code.** Multiple design docs covering RPCs, persistence, deployment, testability. The owner has clearly thought about the system end-to-end before typing.
- **Correct architectural instincts.** Decoupling persistence from the algorithm, treating Raft core as a library, isolating protobuf — these are the same patterns used by `etcd/raft` and `hashicorp/raft`.
- **Realistic scope.** The 10-item scope in `README.md` matches what a serious Raft implementation needs (election, replication, persistence, snapshots, membership, dedup, observability, chaos tests).
- **Channel-first concurrency model in the LLD.** The owner is thinking in Go idioms (channel coordination, goroutine per concern) rather than translating Java thread-and-lock patterns directly.
- **Test strategy is first-class.** `testability.md` treats chaos and regression testing as part of the design, not an afterthought. The chaos-config-as-log-entry idea is clever — every node applies the same chaos parameters via the same Raft log they're testing.

---

## Instructions for maintaining this file

This file should be updated whenever any of the following happens. The assistant should proactively offer to update it; the owner can also request updates explicitly.

**Update triggers:**

- A new architectural commitment is made or an existing one is reversed.
- A build sequence step is started or completed — move it from "not done" to "in progress" to "done."
- A new serious issue is discovered, or a listed issue is resolved.
- A design document is added or significantly revised.
- The directory layout changes in a non-trivial way.
- The owner's purpose, goals, or working preferences change.

**Update rules:**

- Keep sections concise. If a section grows past ~15 bullets, that's a signal to consolidate or move detail into a dedicated design doc.
- Preserve the structure (Purpose / Implementation Plan / Current State / Instructions). Add new top-level sections only if they don't fit anywhere else.
- When marking work as done, do not delete the entry — move it to a "what is done" bullet so the project history is visible.
- When recording a resolved issue, do not delete it — move it from "serious issues" to "what is good" with a note on how it was resolved. This preserves the lessons learned.
- If the assistant updates this file, it must read the file first, then make targeted edits — never rewrite the whole file unless the owner explicitly asks for a fresh restructure.
- Be honest. If something is half-done, say half-done. If a previously-decided commitment is being questioned, record that it's being questioned, not that it's still settled.
