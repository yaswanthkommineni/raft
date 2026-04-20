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
    int deleteEntriesUpto(int logIndex){

    }
    int getTerm(int index){

    }
    // returns conflicting index if conflict, else return 0
    // find the mismatch => jump to first index of that mismatching term and return it
    int appendEntries(LogEntry[] entries){
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
    // leader storage should be on the leader state class
    // int[] nextIndex;
    // int[] matchIndex;
    
    // The state machine (volatile + pesistent)
    StateMachine stateMachine;
    Channel backupIndexChannel;

    // check if the lastApplied is upto date with the log then call the StateMachine to serve the query
    QueryResult serveQuery();

    // call deleteEntriesUpto
    int syncLogBackup();

    

    //channel updates
    {
        backupIndexChannel.consume() -> x {
            lastBackedUp = x;
            syncLogBackup();
        }
    }
}