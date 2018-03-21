import lib.*;

/**
 * Thread Handling one Request Vote Call to a specific peer Raft Node
 */
public class ElectionThread extends Thread{
//    private TransportLib lib;
    private RaftNode node; // Reference to the RaftNode who initiate this Thread
    private int src_id; // Source Address (RaftNode ID) --- Message Sender
    private int dest_id; // Destination Address --- Message Receiver
    private RequestVoteArgs args; // args and MethodType
    private Message RequestVoteMessage;
    private Message RequestVoteReplyMessage;
    private RequestVoteReply reply;

    public ElectionThread(RaftNode raft_node, int src, int dest, RequestVoteArgs args){
//        this.lib = trans_lib;
        this.node = raft_node;
        this.src_id = src;
        this.dest_id = dest;
        this.args = args;
    }

    @Override
    public void run(){
        try{
             /* Create Message Packet */
            byte[] payload = BytesUtil.serialize(this.args);
            this.RequestVoteMessage = new Message(MessageType.RequestVoteArgs, this.src_id, this.dest_id, payload);
            /* Applying instance of transportLib, calling sendMessage.
               The sendMessage will block until there is a reply sent back.
             */
            RequestVoteReplyMessage = this.node.lib.sendMessage(RequestVoteMessage);
//            System.out.println("Election Thread Started!");
//            this.node.node_state.set_role(lib.State.state.leader);
//            System.out.println("Election Thread End!");
            /* The Reply might be null and need to check before Use */
            if(RequestVoteReplyMessage == null){
                /* End this Election Thread */
                this.join();
                return;
            }
            this.reply = (RequestVoteReply) BytesUtil.deserialize(RequestVoteReplyMessage.getBody());

            this.node.Lock.lock();

            if(reply.term > node.node_state.currentTerm){
                /* If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower */
                node.node_state.currentTerm = reply.term;
//                System.out.print(System.currentTimeMillis() + "Node " + this.node.getId() +" Role From "+this.node.node_state.get_role());
                node.node_state.set_role(lib.State.state.follower);
//                System.out.println(System.currentTimeMillis() + "To " + this.node.node_state.get_role());
                node.node_state.votedFor = null;
                node.votes_count = 0;
            }
            else if(reply.term < node.node_state.currentTerm){
                /* Reply From Former Term, Ignore it */
                this.join();
                return;
            }
            else{
                /*
                    Here we need to Make Sure that Current Raft Node is still Candidate
                    Because During ELection, If AppendEntries RPC received from new leader:
                        This Server needs to convert to follower
                 */
                synchronized (node.node_state) {
                    if (reply.voteGranted && (node.node_state.get_role() == lib.State.state.candidate)) {
                        node.votes_count++;
                        node.if_received_RPC = true;
                    /* If votes received from majority of servers: become leader */
                        if (node.votes_count > node.majority) {
                            node.node_state.set_role(lib.State.state.leader);
                        /* Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server; */
                        /* Initialize 'nextIndex' */
                            int prevLastIndex = node.node_state.log.peekLast() == null ? 0 : node.node_state.log.peekLast().index;

                            for(int i=0; i<node.num_peers; i++){
                                node.node_state.nextIndex[i] = prevLastIndex + 1;
                            }

                            node.generateHeartBeat();
                            System.out.println(System.currentTimeMillis() + " Node" + node.getId() + " Is Leader Now " + "Term: " + node.node_state.currentTerm + " Votes: " + node.votes_count);
                            /* Wake Up Immediately */
                            node.node_state.notify();
                        }
                    }
                }
            }

            this.node.Lock.unlock();

            this.join();
            return;
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
