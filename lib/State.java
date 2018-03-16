package lib;

import java.util.ArrayList;
import java.util.LinkedList;

/**
 * Persistent & Volatile state on servers
 */
public class State {
    public enum state{follower, candidate, leader}
    private state node_state = state.follower;
    public int currentTerm;
    public Integer votedFor = null; // Candidate ID that received vote in $Current Term$, (or null if none<hasn't voted yet>)
    public LinkedList<LogEntries> log; //

    /* Volatile state on all Servers */
    public int commitIndex; // index of highest log entry known to be committed (initialized to 0, increases monotonically)
    public int lastApplied; // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

    /* Volatile state on leader */
    public int[] nextIndex; // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
    public int[] matchIndex; //for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

    public State(int num_peers){
        this.currentTerm = 0;
        log = new LinkedList<LogEntries>();

        commitIndex = 0;
        lastApplied = 0;

         /*
            When leader comes to power, initializes all nextIndex values to the index just after the last one in its log
         */

        nextIndex = new int[num_peers];
        matchIndex = new int[num_peers];
    }

    public state get_role(){

        return this.node_state;
    }

    public void set_role(state new_role){

        this.node_state = new_role;
    }
}
