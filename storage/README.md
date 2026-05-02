# storage

Implementations of the `raft.LogStore` interface.

- `memory/` — volatile in-memory log, used for tests.
- `disk/` — durable on-disk log for production.
