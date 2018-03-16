package lib;

import java.io.Serializable;

/**
 * This class is a wrapper for packing all the result information that you
 * might use in your own implementation of the AppendEntries RPC call, and also
 * should be serializable to return by remote function call.
 *
 */
@SuppressWarnings("serial")
public class AppendEntriesReply implements Serializable {
    public int term; // The dest Node's currentTerm, for candidate to update itself
    public boolean success; // true if follower contained entry matching prevLogIndex and prevLogTerm( Send in AppendEntriesArgs )

    public AppendEntriesReply(int term, boolean if_success) {
        this.term = term;
        this.success = if_success;
    }
}
