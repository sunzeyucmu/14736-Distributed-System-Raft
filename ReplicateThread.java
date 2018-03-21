import lib.*;

import java.rmi.RemoteException;
import java.util.ArrayList;

/**
 * Thread Handling one AppendEntries RPC Call to a specific peer Raft Node
 */
public class ReplicateThread extends Thread {
    private RaftNode node; // Reference to the RaftNode who initiate this Thread
    private int lastLogIndex;

    public ReplicateThread(RaftNode raft_node, int lastLogIndex) {
//        this.lib = trans_lib;
        this.node = raft_node;
        this.lastLogIndex = lastLogIndex;
    }

    @Override
    public void run() {
        try {
            while (true) {
            /* send AppendEntries RPCs in parallel */
                for (int i = 0; i < this.node.num_peers; i++) {
                    if (i == this.node.getId()) continue;
                /* If last logIndex ≥ nextIndex for a follower i:
                   send AppendEntries RPC with log entries starting at nextIndex[i]
                 */
                    this.node.Lock.lock();

                /* Make Sure Current Raft Node is still Leader
                   < Leader Send RPC!!!>
                 */
                    if (this.node.node_state.get_role() != lib.State.state.leader) {

                        this.node.Lock.unlock();

                        this.join();
                        return;
                    }

                    if (lastLogIndex >= this.node.node_state.nextIndex[i]) {
                        int prevLastIndex = this.node.node_state.nextIndex[i] - 1;
                        int prevLastTerm = prevLastIndex == 0 ? 0 : this.node.node_state.log.get(prevLastIndex - 1).term;
                        ArrayList<LogEntries> entries = this.node.logStartFrom(this.node.node_state.log, this.node.node_state.nextIndex[i]);
                        AppendEntriesArgs req_args = new AppendEntriesArgs(this.node.node_state.currentTerm, this.node.getId(), prevLastIndex, prevLastTerm, entries, this.node.node_state.commitIndex);
                    /* Start a Thread handling this RPC call */
                        AppendEntriesThread thread = new AppendEntriesThread(this.node, this.node.getId(), i, req_args);
                        thread.start();
                        System.out.println(System.currentTimeMillis() + " Node" + this.node.getId() + " Sent AppendEntries RPC to Node" + i + " " + prevLastIndex + " " + prevLastTerm + " " + entries.size());
                    }

                    this.node.Lock.unlock();

                }
            /* Wait for 50ms for all RPC call Threads to finish*/
                long t0 = System.currentTimeMillis();
                while ((System.currentTimeMillis() - t0) < 100) {

                }
            /*
                If there exists an N such that N > commitIndex,
                a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
                        set commitIndex = N (§5.3, §5.4).
                TODO: Currently for Simplicity, take 'N = lastLogIndex
             */

//            this.Lock.lock();

                for (int N = this.node.node_state.commitIndex + 1; N <= lastLogIndex; N++) {
//                int N = lastLogIndex;
//                if(N > this.node_state.commitIndex){
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
//                            reply = new StartReply(lastLogIndex, this.node_state.currentTerm, true);
//                            return reply;
                    }

                }

                int cur_command_Count = 0;
                for(int t=0; t<node.num_peers; t++){
                    if(node.node_state.matchIndex[t] >= lastLogIndex){
                        cur_command_Count ++;
                    }
                }

                if(cur_command_Count == node.num_peers){
                /* This Thread Have Replicated All Current Entry */
                    this.join();
                    return;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
