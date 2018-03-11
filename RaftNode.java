import lib.*;

import java.rmi.RemoteException;

public class RaftNode implements MessageHandling {

    class LogEntry {
        // TODO: index necessary ?
        private int term;
        private int state;
        private int index;
        public LogEntry(int term, int state, int index) {
            this.state = state;
            this.term = term;
            this.index = index;
        }

        public int getTerm() {
            return term;
        }

        public int getState() {
            return state;
        }
    }

    private int id;
    private static TransportLib lib;
    private int port;
    private int num_peers;

    private boolean isLeader = false;
    private int currentTerm = 0;
    private int votedFor = -1;   // -1 if no candidate requested for vote yet

    private LogEntry[] logs = {};
    // Starting from 1, according to the paper
    private int commitIndex = 0;

    //TODO: Other states

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        /* Create a new instance of TransportLib */
        lib = new TransportLib(port, id, this);
        this.port = port;
    }

    public void requestVote() {
        currentTerm += 1;
        byte[] body = BytesUtil.serialize(new RequestVoteArgs(currentTerm, id, commitIndex, logs[commitIndex-1].getTerm()));
        // TODO: Boradcast by a Loop or sth ; Specify dest_addr
        Message reqMsg = new Message(MessageType.RequestVoteArgs,  port, 1, body);

        try {
            Message respMsg = lib.sendMessage(reqMsg);
            RequestVoteReply reply = (RequestVoteReply) BytesUtil.deserialize(respMsg.getBody());
        } catch (RemoteException e) {
            e.printStackTrace();
        }
        // TODO:
    }

    /*
     *call back.
     */
    @Override
    public StartReply start(int command) {
        return null;
    }

    @Override
    public GetStateReply getState() {
        return null;
    }

    @Override
    public Message deliverMessage(Message message) {
        return null;
    }

    //main function
    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    }
}
