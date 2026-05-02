# transport/inmem

In-process transport. "Sending" a message means pushing it onto the target node's channel. No serialization, no network. Used for multi-node tests in a single `go test` binary and for fault injection.
