## Deployment Model

**Container Structure:**
- One container per node (Storage + Raft server)
- Image contains Raft server binary

**Startup Sequence:**
1. Read env/config file
2. Build config struct
3. Initialize Raft node from image
4. Mount config file at runtime

## Node Initialization

**Startup Logic:**
```
if has storage:
    recover from persistent state
else if no reference node:
    bootstrap (become leader)
else:
    start idle + send addNode log to the reference
```

### Case 1: Bootstrap (First Node)
1. Start node
2. No storage exists
3. No reference node configured
4. Initialize cluster with itself as single member
5. Becomes leader

### Case 2: Joining Existing Cluster (Reference exist)
1. Start node (not part of cluster yet)
2. Operator sends add node log to the reference node via admin API
3. Leader commits membership change to log
4. Node is now officially part of cluster
5. Leader starts sending AppendEntries with:
   - Logs (if node is reasonably behind)
   - InstallSnapshot (if too far behind)
6. Node catches up to cluster state

## Membership Management

**Adding/Removing Nodes:**
- Manual operation via Put API
- Requires encrypted admin credentials in clientToken
- Only leader processes membership changes
- Changes are committed through normal Raft log

**Node Reference Format:**
- address + port