import lib.*;

import java.rmi.RemoteException;
import java.util.ArrayList;

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
//
            /* Create Message Packet */
            byte[] payload = BytesUtil.serialize(this.args);
            this.AppendEntriesMessage = new Message(MessageType.AppendEntriesArgs, this.node.getId(), this.dest_id, payload);

//            System.out.println(System.currentTimeMillis() + " Node" + this.node.getId() + " Sent AppendEntries RPC to Node" + dest_id + " " + args.prevLogIndex + " " + args.prevLogTerm + " " + args.entries.size());
            /* Applying instance of transportLib, calling sendMessage.*/
            try {
                AppendEntriesReplyMessage = this.node.lib.sendMessage(AppendEntriesMessage);
            }
            catch(Exception e){

                System.out.println(System.currentTimeMillis()+" Node "+src_id+" AppendEntries to Node "+dest_id + " Failed to SenT!" + args.prevLogIndex+" "+args.prevLogTerm+" "+args.entries.size());

                this.join();
                return;
            }

            /* The Reply might be null and need to check before Use */
            if(AppendEntriesReplyMessage == null){
//                        System.out.println(System.currentTimeMillis()+" Node "+this.node.getId()+" Append RPC to Node "+dest_id + " Return NULL!" + args.prevLogIndex+" "+args.prevLogTerm+" "+args.entries.size());
                this.join();
                return;
            }
            else{
                this.node.Lock.lock();

                AppendEntriesReply reply = (AppendEntriesReply) BytesUtil.deserialize(AppendEntriesReplyMessage.getBody());


                if(reply.term > node.node_state.currentTerm){
                    /* If RPC response contains term T > currentTerm: set currentTerm = T, convert to follower */
                    node.node_state.currentTerm = reply.term;
//                          System.out.print(System.currentTimeMillis() + "Node " + this.node.getId() +" Role From "+this.node.node_state.get_role()+" ");
                    node.node_state.set_role(lib.State.state.follower);
//                          System.out.println(System.currentTimeMillis() + "To " + this.node.node_state.get_role());
                    node.node_state.votedFor = null;
                    node.votes_count = 0;

                    node.Lock.unlock();

                    this.join();
                    return;
                }

                if(reply.success == false){
                    /* AppendEntries Rejected, Must due to Consistency Check Failure
                       decrement nextIndex and retry
                     */
//                    System.out.println(System.currentTimeMillis()+" Node "+this.node.getId()+" Append RPC to Node "+dest_id + " rejected!" + args.prevLogIndex + " " + args.prevLogTerm + " " + args.entries.size());
                    node.node_state.nextIndex[dest_id] = node.node_state.nextIndex[dest_id] - 1;

                    node.Lock.unlock();

                    this.join();
                    return;
                }
                else {
                    /*
                       Append Entries Succesfully!
                       update nextIndex and matchIndex
                     */

//                    System.out.println(System.currentTimeMillis() + " Node " + this.node.getId() + " Append RPC to Node " + dest_id + " Succeeded!");
                    if (args.entries.size() > 0) {
//                            for (LogEntries e : args.entries) {
//                                e.print();
//                            }
                        node.node_state.matchIndex[dest_id] = args.entries.get(args.entries.size() - 1).index;
                        node.node_state.nextIndex[dest_id] = args.entries.get(args.entries.size() - 1).index + 1;

                        /*
                             If there exists an N such that N > commitIndex,
                                a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                                        set commitIndex = N (§5.3, §5.4).
                         */

                        int lastLogIndex = this.node.node_state.log.peekLast() == null ? 0 : this.node.node_state.log.peekLast().index;

                        for (int N = this.node.node_state.commitIndex + 1; N <= lastLogIndex; N++) {
                            int match_count = 0;
                            for (int i = 0; i < this.node.num_peers; i++) {
                                if (this.node.node_state.matchIndex[i] >= N) {
                                    match_count++;
                                }
                            }
                                /*
                                    Only log entries from the leader’s current term are committed by counting replicas;
                                    Once an entry from the current term has been committed in this way,
                                    Then all prior entries are committed indirectly
                                 */
                            if (match_count > this.node.majority && (this.node.node_state.log.get(N - 1).term == this.node.node_state.currentTerm)) {
                                /* Apply Entry to State Machine */
                                for (int k = this.node.node_state.commitIndex + 1; k <= N; k++) {
                                    ApplyMsg msg = new ApplyMsg(this.node.getId(), k, this.node.node_state.log.get(k - 1).command, false, null);
                                    try {
                                        this.node.lib.applyChannel(msg);
//                                        System.out.println(System.currentTimeMillis() + "Node:" + this.node.getId() + " Role: " + this.node.node_state.get_role() + " index: " + this.node.node_state.log.get(k - 1).index + " term: " + this.node.node_state.log.get(k - 1).term + " command: " + this.node.node_state.log.get(k - 1).command + " Has been Committed");
                                    } catch (RemoteException e) {
                                        e.printStackTrace();

                                        this.node.Lock.unlock();

                                        this.join();
                                        return;
                                    }
                                }
                                this.node.node_state.commitIndex = N;
                                this.node.node_state.lastApplied = N;
                            }

                        }
                    }
                }
            }

            node.Lock.unlock();

            this.join();
            return;
        }
        catch (Exception e){
            e.printStackTrace();
        }
    }

}
