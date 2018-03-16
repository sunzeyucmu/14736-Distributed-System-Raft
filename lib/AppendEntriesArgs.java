package lib;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Invoked by leader to replicate log entries (§5.3);
 * also used as heartbeat (§5.2)
 */
@SuppressWarnings("serial")
public class AppendEntriesArgs implements Serializable{
    public int term; //Leader's Term
    public int leaderId; // Current Term Leader's Id,  so follower can redirect clients
    /* index & term(of related Entry) of log entry immediately preceding new ones */
    public int prevLogIndex;
    public int prevLogTerm;

    public ArrayList<LogEntries> entries; // log entries to store ($$$empty for heartbeat$$$; may send more than one for efficiency)
    public int leaderCommit; // leader’s commitIndex

    public AppendEntriesArgs(int term, int leader_id, int prev_index, int prev_term, ArrayList<LogEntries> logs, int leader_commit){
        this.term = term;
        this.leaderId = leader_id;
        this.prevLogIndex = prev_index;
        this.prevLogTerm = prev_term;
        this.entries = logs;
        this.leaderCommit = leader_commit;
    }

}
