## Internal (node <-> node)

### RequestVote
Request:
- term
- candidateId
- lastLogIndex
- lastLogTerm

Response:
- term
- voteGranted

### AppendEntries
Request:
- term
- leaderId
- prevLogIndex
- prevLogTerm
- entries[]
- leaderCommit

Response:
- term
- success

### InstallSnapshot
Request:
- term
- leaderId
- lastIncludedIndex
- lastIncludedTerm
- offset
- data[]
- done

Response:
- term

## External (client <-> cluster)

### Put
Request:
- key
- value
- clientToken
- sequenceNum

Response:
- success
- leaderId (for redirect)

### Get
Request:
- key

Response:
- value
- found
- leaderId (for redirect)

### Delete
Request:
- key
- clientToken
- sequenceNum

Response:
- success
- leaderId (for redirect)

### CAS (Compare-And-Swap)
Request:
- key
- expectedValue
- newValue
- clientToken
- sequenceNum

Response:
- success
- leaderId (for redirect)

Note: Only leader handles Put/Delete/CAS APIs