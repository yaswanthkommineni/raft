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
    // apply membership config change
    int apply(LogEntry entry){

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
    // find the mismatch => jump to first index of that mismatching term and return it
    int appendEntries(LogEntry[] entries, int prevLogIndex, int prevLogTerm){
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

    void run(){
        // at any point of time only one request could be served
        while(true){
            // listen to the channel messages here
            context.appendEntriesRequestChannel -> x{

            }
            context.voteRequestChannel -> x{

            }
            context.writeRequestChannel -> x{
                // check and apply if membership change
                // append to log
            }
            context.readRequestChannel -> x{
                // create a no-op entry and only respond after that got committed
            }
        }
    }
    
    
}

class FollowerState extends NodeState{
    TimeStamp lastHeartbeatTime;
    Channel heartBeatTimeoutChannel;
    NodeContext context;

    NodeState run(){


        // at any point of time only one request could be served
        while(true){
            // listen to the channel messages here
            // there is no reason to support multiple appendEntries requests at a time
            // respond first and then do the syncrhonous log patching
            context.appendEntriesRequestChannel -> x{
                /*
                    validate the term and respond with term if the term is older
                    update heartBeat() and schedule a timeout;
                    respond false if no entry exist at the prevLogIndex
                    if log exist, respond true and then patch the local log
                    if conflict, respond with the first log index of the conflicting term
                */
            }
            context.voteRequestChannel -> x{
                /*

                */
            }
            context.writeRequestChannel -> x{
                // redirect to leader
            }
            context.readRequestChannel -> x{
                // redirect to leader
            }
            context.lastHeartbeatTime -> x{
                /*
                    Check if the timeout
                */
            }
        }
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