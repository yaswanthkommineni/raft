# transport

Implementations of the `raft.Transport` interface.

- `inmem/` — in-process delivery via Go channels. Fast and deterministic, used for tests.
- `grpc/` — production network transport over gRPC.
