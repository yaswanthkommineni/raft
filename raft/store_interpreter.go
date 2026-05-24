package raft

import (
	"log/slog"
)

/*
Intercepts some of the functions to the store.
Detects if the log entry is of membership-change type and applies it
to the in-memory Membership before delegating to the underlying Store.

Why: keep membership-dispatch logic in one place outside Store
(separation of concerns). Store stays dumb; algorithm-level routing
lives here.

Concurrency: relies on the Store single-writer invariant — at most one
goroutine ever calls PatchEntries/TruncateFrom at a time, which is also
the only goroutine that mutates Membership. See Membership doc.
*/
type StoreInterpreter struct {
	Store
	Membership *Membership
	logger     *slog.Logger
}

func NewStoreInterpreter(store Store, membership *Membership, logger *slog.Logger) *StoreInterpreter {
	return &StoreInterpreter{
		Store:      store,
		Membership: membership,
		logger:     logger.With("component", "store_interpreter"),
	}
}

// PatchEntries stages membership-change entries on a working clone of
// Membership first; only after the underlying Store write succeeds do we
// adopt the new state into the real Membership. If anything fails — a
// single apply, or the Store write — the real Membership is untouched,
// keeping in-memory state in lockstep with what is on disk.
func (storeInterpreter *StoreInterpreter) PatchEntries(entries []LogEntry) error {
	storeInterpreter.Membership.RwMu.Lock()
	defer storeInterpreter.Membership.RwMu.Unlock()
	working := storeInterpreter.Membership.cloneState()
	for i := range entries {
		if entries[i].LogType != LogTypeMembership {
			continue
		}
		storeInterpreter.logger.Info("staging membership change", "log_index", entries[i].LogIndex)
		if err := working.apply(&entries[i]); err != nil {
			storeInterpreter.logger.Error("failed to stage membership change", "log_index", entries[i].LogIndex, "error", err)
			return err
		}
	}
	if err := storeInterpreter.Store.PatchEntries(entries); err != nil {
		return err
	}
	storeInterpreter.Membership.swapStateFrom(working)
	return nil
}

// TruncateFrom stages the membership revert on a clone first, writes the
// underlying store, and only on success swaps the clone in. Mirrors the
// PatchEntries pattern so the failure modes are symmetric: at no point
// does in-memory Membership get ahead of (or behind) what is on disk.
func (storeInterpreter *StoreInterpreter) TruncateFrom(index LogIndex) error {
	storeInterpreter.Membership.RwMu.Lock()
	defer storeInterpreter.Membership.RwMu.Unlock()
	working := storeInterpreter.Membership.cloneState()
	if err := working.revertMembershipChangesTill(index); err != nil {
		storeInterpreter.logger.Error("failed to stage membership revert", "index", index, "error", err)
		return err
	}
	if err := storeInterpreter.Store.TruncateFrom(index); err != nil {
		return err
	}
	storeInterpreter.Membership.swapStateFrom(working)
	return nil
}
