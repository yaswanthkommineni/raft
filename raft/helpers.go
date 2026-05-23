package raft

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"time"
)

func RandomDuration(min, max time.Duration) time.Duration {
	return min + time.Duration(rand.IntN(int(max-min)+1))
}

// EncodeMembershipChange serializes a MembershipChange into the byte payload
// stored in LogEntry.Data for entries with LogType == LogTypeMembership.
// Keep this and DecodeMembershipChange in lockstep.
func EncodeMembershipChange(change MembershipChange) ([]byte, error) {
	return json.Marshal(change)
}

// DecodeMembershipChange parses a MembershipChange from a LogEntry.Data
// payload. Returns an error on empty input or malformed JSON; the caller
// should treat either as a corrupt log entry.
func DecodeMembershipChange(data []byte) (MembershipChange, error) {
	if len(data) == 0 {
		return MembershipChange{}, fmt.Errorf("DecodeMembershipChange: empty payload")
	}
	var change MembershipChange
	if err := json.Unmarshal(data, &change); err != nil {
		return MembershipChange{}, fmt.Errorf("DecodeMembershipChange: %w", err)
	}
	return change, nil
}


type Stack[T any] []T

func (s *Stack[T]) Push(v T) {
    *s = append(*s, v)
}

func (s *Stack[T]) Pop() (T, bool) {
    if len(*s) == 0 {
        var zero T
        return zero, false
    }

    index := len(*s) - 1
    value := (*s)[index]
    *s = (*s)[:index]
    return value, true
}

func (s *Stack[T]) Peek() (T, bool) {
    if len(*s) == 0 {
        var zero T
        return zero, false
    }

    return (*s)[len(*s)-1], true
}