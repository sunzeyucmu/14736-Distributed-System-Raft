import lib.*;

/**
 * Thread Handling one AppendEntries RPC Call to a specific peer Raft Node
 */
public class AppendEntriesThread extends Thread{
    private RaftNode node; // Reference to the RaftNode who initiate this Thread
    private int src_id; // Source Address (RaftNode ID) --- Message Sender
    private int dest_id; // Destination Address --- Message Receiver
    private AppendEntriesArgs args; // args and MethodType
    private Message AppendEntriesMessage;
    private Message AppendEntriesReplyMessage;
    private AppendEntriesReply reply;

    public AppendEntriesThread(RaftNode raft_node, int src, int dest, AppendEntriesArgs args){
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
            this.AppendEntriesMessage = new Message(MessageType.AppendEntriesArgs, this.src_id, this.dest_id, payload);
            /* Applying instance of transportLib, calling sendMessage.
               The sendMessage will block until there is a reply sent back.
             */
//            System.out.println(System.currentTimeMillis()+" HeartBeat From Node"+src_id+" To Node"+dest_id+" Been Sent!");
            AppendEntriesReplyMessage = this.node.lib.sendMessage(AppendEntriesMessage);

            /* The Reply might be null and need to check before Use */
            if(AppendEntriesReplyMessage == null){
                /* End this Election Thread */
                System.out.println(System.currentTimeMillis()+" Node "+src_id+" Append RPC to Node "+dest_id + " Return NULL!" + args.prevLogIndex+" "+args.prevLogTerm+" "+args.entries.size());
                this.join();
                return;
            }
            this.reply = (AppendEntriesReply) BytesUtil.deserialize(AppendEntriesReplyMessage.getBody());

            node.Lock.lock();

            if(reply.term > node.node_state.currentTerm){
                /* If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower */
                node.node_state.currentTerm = reply.term;
                System.out.print(System.currentTimeMillis() + "Node " + this.node.getId() +" Role From "+this.node.node_state.get_role()+" ");
                node.node_state.set_role(lib.State.state.follower);
                System.out.println(System.currentTimeMillis() + "To " + this.node.node_state.get_role());
                node.node_state.votedFor = null;
                node.votes_count = 0;

                node.Lock.unlock();

                this.join();
                return;
            }
            /* TODO: Current this AppendEntries can only handle 'HeartBeat' (AppendEntriesRPC with empty log entries)*/
            if(reply.success == false){
                /* AppendEntries Rejected, Must due to Consistency Check Failure
                   decrement nextIndex and retry
                 */
                System.out.println(System.currentTimeMillis()+" Node "+src_id+" Append RPC to Node "+dest_id + " rejected!" + args.prevLogIndex+" "+args.prevLogTerm+" "+args.entries.size());
                node.node_state.nextIndex[dest_id] = node.node_state.nextIndex[dest_id] - 1;
            }
            else{
                /*
                     update nextIndex and matchIndex
                     ( )
                 */
                if(args.entries.size() > 0){
                    System.out.println(System.currentTimeMillis()+" Node "+src_id+" Append RPC to Node "+dest_id + " Succeeded!");
                    for(LogEntries e : this.args.entries){
                        e.print();
                    }
                    node.node_state.matchIndex[dest_id] = args.entries.get(args.entries.size()-1).index;
                    node.node_state.nextIndex[dest_id] = args.entries.get(args.entries.size()-1).index+1;
                }

                else{
                    System.out.println(System.currentTimeMillis()+" Node "+src_id+" HeartBeat to Node "+dest_id + " Succeeded! " + args.prevLogIndex+" "+args.prevLogTerm+" "+args.entries.size() );
                }

            }

            node.Lock.unlock();

            this.join();
            return;
        }
        catch(Exception e){
            e.printStackTrace();
        }
    }

}
