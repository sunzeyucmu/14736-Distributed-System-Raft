package lib;

import java.io.Serializable;

/**
 * This class is a wrapper for packing all the arguments that you might use in
 * the RequestVote call, and should be serializable to fill in the payload of
 * Message to be sent.
 *
 */
public class RequestVoteArgs implements Serializable {

    private int term;
    private int candidateId;
    private int lastLogIndex;
    private int lastLogTerm;

    public RequestVoteArgs(int term, int candidateId, int lastLogIndex, int lastLogTerm) {
        this.term = term;
        this.candidateId = candidateId;
        this.lastLogIndex = lastLogIndex;
        this.lastLogTerm = lastLogTerm;
    }


}
