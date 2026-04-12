## Testing Strategy

**Principle:** All components in the system should be testable in isolation and integration.

## Unit Tests

**Coverage:**
- Individual Raft state machine transitions
- Log operations (append, truncate, compaction)
- RPC handler logic
- Client request deduplication
- State persistence and recovery

## Integration Tests

**Scenarios:**
- Leader election (normal and partitioned)
- Log replication across cluster
- Snapshot installation
- Membership changes (add/remove nodes)
- Client request handling and redirection
- Follower/candidate/leader role transitions

## Regression Tests

**Automated Test Suite:**
- Separate script to trigger comprehensive corner case tests
- Tests verify:
  - Linearizability (operations appear atomic and sequential)
  - Availability (cluster handles failures gracefully)
  - Consistency (all nodes converge to same state)
- Continuous load: millions of requests under various failure conditions

**Failure Injection:**

**Trigger Mechanism:**
- Chaos configuration sent as special log entry to cluster
- All nodes apply the same chaos parameters
- Chaos log entry contains:
  - Network settings: average delay (ms), failure probability (%)
  - Node crash rate: average time between crashes
  - Network partition: partition probability
- The chaos config will be cleared once a clear chaos log entry is appended

**Failure Types:**
- Network delays/drops: RPC interceptor applies delay/drop before message handling
- Node crashes: Background timer triggers SIGKILL, tests recovery on restart
- Partitions: Network layer drops messages between partition groups (must be symmetric)
- Disk failures: Storage layer returns errors (must persist until chaos cleared)

## Manual Verification

**Known Edge Cases:**
Document rare edge cases from common Raft implementations:

For each edge case:
1. Description of the scenario
2. Why the implementation handles it correctly
3. Assumptions made
4. Test verification approach

**Examples:**
- Split-brain prevention during network partition
- Log divergence after leader crash
- Stale read prevention
- Duplicate client request handling
- Snapshot while log replication in progress