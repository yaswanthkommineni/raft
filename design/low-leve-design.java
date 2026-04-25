/*
LLD is done in Java to have better understanding. However, components like channels will be represented here.
*/

enum SYSTEM_TYPES{
    METADATA_STORAGE,
}


// method that returns the correct state machine based on type
StateMachine getStateMachine(SYSTEM_TYPES type){
}

interface StateMachine {
    int applyLogEntry(){

    }
    // not yet decided how the stream works
    // will be added more details later
    int stream(){

    }
    QueryResult queryResult(){

    }
    Channel backupIndexChannel(){

    }
    /*
        Internal implementation:
        Contains two state machines one volatile (hashmap) and the other is persistent storage
        periodically delete the volatile storage flush it to persistent storage
        // this detailed design will be designed later
    */
}

// substitution for go's channel
interface Channel {
    String consume();
}

// the result can be depending on the type of storage we are having
interface QueryResult{

}

// persistent storage to store config details
class MembershipConfig{
    // maps nodeId to it's info
    Map <int, NodeInfo> nodes;
    int lastAppliedIndex;

    // non persistent info
    Map <int, Channel> membershipChangeListeners;
    // apply membership config change
    int apply(LogEntry entry){
        // apply to the local state
        // send the membership update to the listeners
    }
    // send messages to these channels when teh config changed
    int registerConfigChangeListener(Channel channel){

    }
    int deRegisterChannel(int channelId){
        // remove channel from the map
    }
}

class LogEntry{
    int term;
    int logIndex;
    // string that contains the log 
    // this can contain various types as we are supporting multiple storage mechanisms
    // the exact log type will be decoded in the implementation of the state machine
    String jsonString;
}

/*
Efficiency => avoid O(n) scanning in case of finding the mismatches
*/
interface Log {
    // returns 0 if append is successful
    int append(LogEntry entry){

    }
    LogEntry getLastLog(){

    }
    // check: should not delete the membership logs until it is applied to the membership config
    int deleteEntriesUpto(int logIndex){

    }
    int getTerm(int index){

    }
    // returns conflicting index if conflict, else return 0
    // find the mismatch => jump to first index of that mismatching term and return then
    // if not mismatch replace whatever after the prevLogIndex with the given entries
    int, int appendEntries(LogEntry[] entries, int prevLogIndex, int prevLogTerm){
        // while appending, check if the log entry is of membership change type and apply to the system if it is
        // incase of deleting logs because of conflict, don't forget to update the membership config if the deleted log is membership config
    }
    LogEntry[] getLogEntries(int startIndex, int endIndex){
        
    }
}


/*
Gaps:
How to restructure the leader data structures when the config is changed?
*/

class Storage {
    // persistent Storage
    int term;
    int votedFor;
    Log log;
    int lastBackedUp;
    // non persistent Storage
    int lastCommitted;
    int lastApplied;
    // non-persistent leader storage;

    // The state machine (volatile + pesistent)
    StateMachine stateMachine;
    Channel backupIndexChannel;

    // check if the lastApplied is upto date with the log then call the StateMachine to serve the query
    QueryResult serveQuery();

    // call deleteEntriesUpto
    int syncLogBackup();

    // call this function to update commit index
    int updateCommitIndex() {
        // update the commitIndex atomic operation
        // acquire mutex lock while doing this, so that two go-routines won't be triggered applying the same entries
        // initiate a go-routine to apply log entries from x to y
    }

}

class NodeContext{
    Channel appendEntriesRequestChannel, voteRequestChannel, writeRequestChannel, readRequestChannel;
    NodeState state;
    Storage storage;

    // persistent storage
    SYSTEM_TYPES systemType;
    MembershipConfig config;
}


// the main class
class RaftNode{
    
    void boot(){
        // initialize the NodeContext from persistent storage and create fresh ones for the ones that are not persistent
        // start background processes like storage syncup, health check reporting...
        // create RPC services
    }

    void run(){
        while(NodeContext.state != AbortState){
            NodeContext.state = NodeContext.state.run();
        }
    }

    void shutDown(){
        // for graceful shutdown
    }

}


interface NodeState{
    run();
}

class AbortState extends NodeState{

}

class LeaderState extends NodeState{
    // leader state
    int[] nextIndex;
    int[] matchIndex;

    // send heart beat every heartBeatFrequency seconds
    int heartBeatFrequency = baseTiemout/3;

    // follower to thread mapper for each follower
    Map <int, Thread> threadMapper;
    Thread configListenerThread;

    // config change listeners
    Channel configChangeChannel, leaderExpiredChannel, commitUpdatedChannel;

    // For every log index, upon commit confirmation, the updates should be sent to the mapped channel
    Queue <<int, Channel>> commitConfirmationChannels;

    BroadCastChannel logUpdateChannel;


    int leaderStartup(){
        // push a new message to the writeRequestChannel with emtpy write (no-op entry) and no response channel
        // start threads that execute follower communication and save the thread in the map
        // register a chanenl for config changes (configChangeChannel)
        // create a dummy followerif there are no followers
    }


    void followerCommunication(){
        int lastHeartBeatTime;
        Channel heartBeatExpireChannel;
        while(true){
            // all are asyncrhonous
            logUpdateChannel -> x{
                // log has been updated, send append entries by calling sendAppendEntriesRPC
            }
            heartBeatExpireChannel ->x {
                // check if it is actually expired and send appendEntries if it did
            }
            responseChannel -> x{
                /*
                    check if leader is updated and put the message in channel leaderExpiredChannel
                    update the follower related details
                    note: while updating either do CAS operations or use mutex locks to avoid race conditions
                    call commitUpdateCheck() asynchronously
                */
            }
        }
    }

    void commitUpdateCheck(){
        /*
            checks if the commit is updated and if updated, put it in the channel commitUpdatedChannel
            also only send update if the commit index belongs to the current term
        */
    }

    int sendAppendEntriesRPC(int followerId, Channel responseChannel, Channel heartBeatExpireChannel){
        // set the time out after heartBeatFrequency + currentTime using heartBeatExpireChannel
        // send the RPC and wait for result and send response over the channel
    }

    // graceful leader shutdown
    int leaderShutdown(){
        // immediately shutdown go-routines that are reading from channels appendEntries, voteRequest, writeRequest, readRequest, and configChangeChannel
        // immediately shutdown all the follower communication go-routines
        // continously poll messages commitConfirmationChannels and respond {committed: false} to threadMapper
        // deregister config change channel
    }

    void run(){
        // at any point of time only one request could be served
        while(true){
            // all are asynchronous
            context.appendEntriesRequestChannel -> x{
                /*
                    Check if the term is greater than the current term
                    if it is, put the new leaderId in the leaderExpiredChannel after responding false
                    else respond false with the updated term
                */
            }
            context.voteRequestChannel -> x{
                /*
                    Check if the term is greater than the current term if not respond false
                    if it is, delegate to handleVoteRequest() and push a message to leader changed with empty leaderId;
                */
            }
            context.writeRequestChannel -> x{
                /*
                    push an entry in commitConfirmationChannel with response channel as the channel passed in the write request
                    append to log
                    broadcast in logUpdateChannel
                */
            }
            context.readRequestChannel -> x{
                /*
                    Create a no-op entry in the writeRequestChannel and pass a new channel as response channel
                    now create a new goroutine that listens to this new channel and after commit confirmation came it reads the value and returns it
                    note: this newly created go-routine won't be immediately shut-down once the leader changed
                */
            }
            configChangeChannel -> x {
                /*
                    detect deleted followers => kill the respective followerCommunication go-routine
                    detect newly added followers => create the respective followerCommunication go-routine
                */
            }
            commitUpdatedChannel -> x {
                /*
                    update the commitIndex variable
                    Pop items from the queue commitConfirmationChannels and put confirmation messages in the popped channel
                */
            }
            leaderExpiredChannel -> x {
                /*
                    Update the term and leader
                    call leaderShutdown();
                    then return FollowerState;
                */
            }
        }
    }

}


void handleVoteRequest(VoteRequest request, Channel responseChannel, Storage storage){
     /*
        validate for term and return false along with the latest term if the requestor is outdated
        if the term is same as the current term then check if votedFor is null and proceed for log validation if null return false else
        Check if the node asking for vote is as upto date as the follower and then grant the vote
        update term 
    */
}

// one request handled at a time so no concurrency problem, does it impact performance though?
class FollowerState extends NodeState{
    TimeStamp lastHeartbeatTime;
    Channel heartBeatTimeoutChannel;
    NodeContext context;
    Channel localAppendEntriesRequest;


    int heartBeatTimeout = baseTiemout + random;

    NodeState run(){


        // at any point of time only one request could be served
        while(true){
            // listen to the channel messages here
            // there is no reason to support multiple appendEntries requests at a time
            // respond first and then do the syncrhonous log patching
            // only one message from the following two channels are processed at a time
            localAppendEntriesRequest -> x {
                /*
                    validate the term and respond with term if the leader term is older
                    respond false if no entry exist at the prevLogIndex
                    if log exist, respond true and then patch the local log
                    if conflict, respond with the first log index of the conflicting term
                    update the commit index
                */
            }
            context.voteRequestChannel -> x{
                // update timer and schedule a new timeout
               // delegate to handleVoteRequest()
            }
            // the channels below this are concurrent and can execute independently
            context.appendEntriesRequestChannel -> x{
                // update lastHeartbeatTime and schedule a timeout at lastHeartbeatTime + timeoutTime;
                // push to localAppendEntriesRequest
            }
            context.writeRequestChannel -> x{
                // redirect to leader
            }
            context.readRequestChannel -> x{
                // redirect to leader
            }
            heartBeatTimeoutChannel -> x{
                /*
                    check if a heartbeat appeared after x - timeoutTime and ignore if it occured
                    else call handleTimeout() and return CandidateState();
                */
            }
        }
    }

    void handleTimeout(){
        /*
            shutdown all the other threads that were initiated
            The requests that are in queue will be handled by the later state
         */
    }
}

class CandidateState extends NodeState{
    TimeStamp lastHeartbeatTime;
    Channel heartBeatTimeoutChannel;
    NodeContext context;
    Channel localAppendEntriesRequest;


    int heartBeatTimeout = baseTiemout + random;

    NodeState run(){

        while(true){
            // listen to the channel messages here
            // there is no reason to support multiple appendEntries requests at a time
            // respond first and then do the syncrhonous log patching
            // only one message from the following two channels are processed at a time
            localAppendEntriesRequest -> x {
                /*
                    validate the term and respond with term if the leader term is older
                    create a goroutine to schedule a requeue for the request 
                    if log exist, respond true and then patch the local log
                    if conflict, respond with the first log index of the conflicting term
                    update the commit index
                */
            }
            context.voteRequestChannel -> x{
               // delegate to handleVoteRequest()
            }
            // the channels below this are concurrent and can execute independently
            context.appendEntriesRequestChannel -> x{
                // update lastHeartbeatTime and schedule a timeout at lastHeartbeatTime + timeoutTime;
                // push to localAppendEntriesRequest
            }
            context.writeRequestChannel -> x{
                // redirect to leader
            }
            context.readRequestChannel -> x{
                // redirect to leader
            }
            heartBeatTimeoutChannel -> x{
                /*
                    check if a heartbeat appeared after x - timeoutTime and ignore if it occured
                    else call handleTimeout() and return CandidateState();
                */
            }
        }
    }

    void handleTimeout(){
        /*
            shutdown all the other threads that were initiated
            The requests that are in queue will be handled by the later state
         */
    }
}

class CandidateState extends NodeState{

}

interface RPCService{
    int startListening();
}

class AppendEntriesResponse{

}

class AppendEntriesRequest{
    // the request information
}

class VoteRequestResponse{

}

class VoteRequest{
    // the request information
}

class Response{
    int code;
    String body;
}


// all requests are validated before processed
// parse the request, create a new callback channel and pass the request object along with the response channel
// wait for the response on the response channel and then serve the request

class InternalRPCService implements RPCService{
    Channel appendEntriesRequestChannel, voteRequestChannel;
    Response appendEntriesRPC(String request){
    }
    Response voteRequestRPC(String request){
    }
}

class ClientRPCService implements RPCService{
    Channel writeRequestChannel, readRequestChannel;
}