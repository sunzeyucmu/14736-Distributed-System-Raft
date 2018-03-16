package lib;

import java.io.Serializable;
/**
 * This class is a wrapper for packing all the result information that you
 * might use in your own implementation of the RequestVote call, and also
 * should be serializable to return by remote function call.
 *
 */
@SuppressWarnings("serial")
public class RequestVoteReply implements Serializable {
    public int term; // currentTerm, for candidate to update itself
    public boolean voteGranted; // true means Candidate received Vote

    public RequestVoteReply(int term, boolean voteGranted) {
        this.term = term;
        this.voteGranted = voteGranted;
    }
}
