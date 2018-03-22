import lib.*;

import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Thread Handling one AppendEntries RPC Call to a specific peer Raft Node
 */
public class ReplicateThread extends Thread {
    private RaftNode node; // Reference to the RaftNode who initiate this Thread
//    private int lastLogIndex;
    private int dest_id;

    public ReplicateThread(RaftNode raft_node, int dest_id) {
//        this.lib = trans_lib;
        this.node = raft_node;
        this.dest_id = dest_id;
    }

    @Override
    public void run() {
        try {
            while (true) {
                this.node.Lock.lock();

                /* Make Sure Current Raft Node is still Leader
                   < Leader Send RPC!!!>
                 */

                if (this.node.node_state.get_role() != lib.State.state.leader) {

                    this.node.Lock.unlock();

                    this.join();
                    return;
                }

                int lastLogIndex = this.node.node_state.log.peekLast() == null ? 0 : this.node.node_state.log.peekLast().index;
                /* If last logIndex ≥ nextIndex for a follower i:
                   send AppendEntries RPC with log entries starting at nextIndex[i]
                 */

                if (lastLogIndex >= this.node.node_state.nextIndex[this.dest_id]) {
                    int prevLastIndex = this.node.node_state.nextIndex[this.dest_id] - 1;
                    int prevLastTerm = prevLastIndex == 0 ? 0 : this.node.node_state.log.get(prevLastIndex - 1).term;
                    ArrayList<LogEntries> entries = this.node.logStartFrom(this.node.node_state.log, this.node.node_state.nextIndex[dest_id]);
                    AppendEntriesArgs req_args = new AppendEntriesArgs(this.node.node_state.currentTerm, this.node.getId(), prevLastIndex, prevLastTerm, entries, this.node.node_state.commitIndex);

//                    this.node.Lock.unlock();

                    /* Create Message Packet */
                    byte[] payload = BytesUtil.serialize(req_args);
                    Message message = new Message(MessageType.AppendEntriesArgs, this.node.getId(), this.dest_id, payload);

                    System.out.println(System.currentTimeMillis() + " Node" + this.node.getId() + " Sent AppendEntries RPC to Node" + dest_id + " " + prevLastIndex + " " + prevLastTerm + " " + entries.size());

                    Message replyMessage = this.node.lib.sendMessage(message);

                    /* The Reply might be null and need to check before Use */
                    if(replyMessage == null){
                        System.out.println(System.currentTimeMillis()+" Node "+this.node.getId()+" Append RPC to Node "+dest_id + " Return NULL!" + prevLastIndex+" "+ prevLastTerm+" "+entries.size());

                    }
                    else{
                        AppendEntriesReply reply = (AppendEntriesReply) BytesUtil.deserialize(replyMessage.getBody());

//                        node.Lock.lock();

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
                            System.out.println(System.currentTimeMillis()+" Node "+this.node.getId()+" Append RPC to Node "+dest_id + " rejected!" + prevLastIndex+" "+prevLastTerm+" "+entries.size());
                            node.node_state.nextIndex[dest_id] = node.node_state.nextIndex[dest_id] - 1;
                        }
                        else{
                            /*
                                Append Entries Succesfully!
                                 update nextIndex and matchIndex
                             */

                            System.out.println(System.currentTimeMillis()+" Node "+this.node.getId()+" Append RPC to Node "+dest_id + " Succeeded!");
                            for(LogEntries e : entries){
                                e.print();
                            }
                            node.node_state.matchIndex[dest_id] = entries.get(entries.size()-1).index;
                            node.node_state.nextIndex[dest_id] = entries.get(entries.size()-1).index+1;

                            /*
                                If there exists an N such that N > commitIndex,
                                a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                                        set commitIndex = N (§5.3, §5.4).
                             */

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
                                            System.out.println(System.currentTimeMillis() + "Node:" + this.node.getId() + " Role: " + this.node.node_state.get_role() + " index: " + this.node.node_state.log.get(k - 1).index + " term: " + this.node.node_state.log.get(k - 1).term + " command: " + this.node.node_state.log.get(k - 1).command + " Has been Committed");
                                        } catch (RemoteException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                    this.node.node_state.commitIndex = N;
                                    this.node.node_state.lastApplied = N;
                                }

                            }
                        }

//                        node.Lock.unlock();
                    }
                }

                node.Lock.unlock();

            /* Wait for 150ms for all RPC call Threads to finish*/
//                long t0 = System.currentTimeMillis();
//                while ((System.currentTimeMillis() - t0) < 100) {
//
//                }
                Thread.sleep(100);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
