package lib;

import java.io.Serializable;

/**
 * This class is a wrapper for packing all the arguments that you might use in
 * the RequestVote call, and should be serializable to fill in the payload of
 * Message to be sent.
 *
 */
@SuppressWarnings("serial")
public class RequestVoteArgs implements Serializable {
    /* Invoke by candidate to gather votes(During Election) */

    public int term; // candidate's term
    public int candidateId; // candidate's ID(server ID)
    public int lastLogIndex; // index of candidate's last log entry
    public int lastLogTerm; // term of candidate's last log entry

    public RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }


}
