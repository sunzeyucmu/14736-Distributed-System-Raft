import lib.*;
import java.rmi.RemoteException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.locks.ReentrantLock;

/*
    For the Election Listening Thread:
     we can declare a class(RaftNode) that implements the Runnable interface.
     That class then implements the run method.
     An instance of the class(RaftNode) can then be allocated,
     passed as an argument when creating Thread, and started.
 */

public class RaftNode implements MessageHandling, Runnable {

//    class LogEntry {
//        // TODO: index necessary ?
//        private int term;
//        private int state;
//        private int index;
//        public LogEntry(int term, int state, int index) {
//            this.state = state;
//            this.term = term;
//            this.index = index;
//        }
//
//        public int getTerm() {
//            return term;
//        }
//
//        public int getState() {
//            return state;
//        }
//    }

    private int id; // id for this Node(Server)
    protected  TransportLib lib; // Instance of TransportLib for communication between peer Raft Nodes
    private int port; // port Number of Controller
    protected int num_peers; // Num of peer RaftNodes existing in the cluster

//    private boolean isLeader = false;
//    private int currentTerm = 0;
//    private int votedFor = -1;   // -1 if no candidate requested for vote yet

    protected State node_state; // State for this RaftNode

    /* A blocking queue is designed to wait for the queue to become non-empty when retrieving an element,
       and wait for space to become available in the queue when storing an element.
     */
    private Thread background_Thread; // Handling all Operation of This Raft Server

    /* Range of Election timeout, Milliseconds based */
    private Random rand = new Random(); // for Generating random TimeOuts
    private static final int election_timeout_lo = 450;
    private static final int election_timeout_hi = 900;
    private int election_timeout;

    /* Heart Interval */
    private static int heartbeat_interval = 90;

    /* Count the Total Votes Received */
    protected int votes_count;
    protected int majority;
//    private LogEntry[] logs = {};

    /* ... */
    public boolean if_received_RPC;

    /* A reentrant mutual exclusion Lock */
    public final ReentrantLock Lock = new ReentrantLock();

    //TODO: Other states

    public RaftNode(int port, int id, int num_peers) {
        this.id = id;
        this.num_peers = num_peers;
        /* Create a new instance of TransportLib */
        lib = new TransportLib(port, id, this);
        this.port = port;

        node_state = new State(num_peers);

        votes_count = 0;
        majority = this.num_peers/2;

        if_received_RPC = false;
        /* Initiate a new thread that creates a background thread
           that will kick off leader election periodically
         */
//        Integer a = null;
//        System.out.println((-1000) % 100);
        background_Thread = new Thread(this); // RaftNode object's run method is invoked when this thread is started
        background_Thread.start();

    }

    /*
     *  Implements the 'run()' method (Thread)
     *
     */
    @Override
    public void run() {

        /* For Followers */
            /* Respond to RPCs from candidates and leaders
               If election timeout elapses without receiving
                    AppendEntriesRPC from current leader or
                    granting vote to candidate:
               convert to Candidate
            */
        try{
            while(true){
                if(node_state.get_role() == State.state.leader){
                    /*
                        Upon election: send initial empty AppendEntries RPCs (heartbeat) to each server;
                        repeat during idle periods to prevent election timeouts
                     */
                    /* Send HeartBeat */
//                    System.out.println(System.currentTimeMillis()+" Node"+this.id+" Role: "+this.node_state.get_role());

                    try {
                        Thread.sleep(heartbeat_interval);
                    }
                    catch (Exception e){
                        e.printStackTrace();
                    }

                    generateHeartBeat();

                }
                else {
                    reset_election_timer();
//                    System.out.println(System.currentTimeMillis()+" Node"+this.id+" Role: "+this.node_state.get_role()+" Election Timeout: "+this.election_timeout);
                    /*
                        Thread>Sleep here may not be a good idea, Because Current Node May Become Leader While sleeping
                        So it needs to wake up Immediately to generate HeartBeat to establish authority
                     */
//                    try {
//                        Thread.sleep(election_timeout);
//                    }
//                    catch (Exception e){
//                        e.printStackTrace();
//                    }
                    synchronized (this.node_state){
                        this.node_state.wait(this.election_timeout);
                    }
//                    System.out.println(System.currentTimeMillis()+" Node"+this.id+" Role: "+this.node_state.get_role()+" Election Timeout End");
                    if(this.node_state.get_role() == State.state.leader)continue;

                    if(if_received_RPC){
                        /* Received RPCs During this Period of Time */
                        if_received_RPC = false; // Reset It
                        continue;
                    }
//                    System.out.println("Election For Node" + this.id + " Started!");
                    /* Convert to Candidate */
                    this.node_state.set_role(State.state.candidate);
                    /* Start Election */
                    System.out.println(System.currentTimeMillis()+" Election For Node" + this.id + " Started!");
                    KickOffElection();
//                    System.out.println("Election For Node" + this.id + " Ended!");
                }
            }
        }
        catch (Exception e){
            e.printStackTrace();
        }
        /* Generate Timeout in range [election_timeout_lo, election_timeout_hi] */
//        election_timeout = rand.nextInt() % (election_timeout_hi - election_timeout_lo) + election_timeout_lo;
//        System.out.println("The Node's Role is: "+this.node_state.get_role());
//        new ElectionThread(this, 0,0,null).start();
//        try {
//            Thread.sleep(300);
//        }
//        catch (Exception e){
//            e.printStackTrace();
//        }
//        System.out.println("The Node's Role is: "+this.node_state.get_role());
//
//        return;
    }

    /* On coversion to Candidate, Start Election : */
    public void KickOffElection(){
        /* 1. Increment CurrentTerm */
        this.node_state.currentTerm ++;
        /* 2. vote for Self */
        this.node_state.votedFor = this.id;
        votes_count ++;
        /* 3. Reset Election Timer */
        reset_election_timer();
        /* 4. Send RequestVote RPCs to all peer servers */
        int lastLogIndex = this.node_state.log.peekLast() == null ? 0 :  this.node_state.log.peekLast().index;
        int lastLogTerm = this.node_state.log.peekLast() == null ? 0 :  this.node_state.log.peekLast().term;
        for(int i=0; i<num_peers; i++){
            if(i == this.id)continue;
            /* TODO: Here the last two params (-1, -1) are for (lastLogIndex, lastLogTerm); Used Just for Now */
            RequestVoteArgs request_args = new RequestVoteArgs(this.node_state.currentTerm, this.id, lastLogIndex, lastLogTerm);
            /* Open an ElectionThread for each RequestVote
             * Because using sendMessage to requestVote will block until there is a reply sent back.
             * */
//            System.out.println("ElectionThread for "+i);
            ElectionThread thread = new ElectionThread(this,this.id,i, request_args);
            thread.start();
        }
    }

    /**
     * send initial empty AppendEntries RPCs(empty logentries) (heartbeat) to each server
     */
    public void generateHeartBeat(){
        /* Make Sure Current RaftNode Still Leader */
        if(node_state.get_role() == State.state.leader){
            ArrayList<LogEntries> logs = new ArrayList<LogEntries>(); // Empty Entries
            for(int i=0; i<num_peers; i++){
                if(i == this.id)continue;
                Lock.lock();

                int prevLastIndex = this.node_state.nextIndex[i] - 1;
                int prevLastTerm = prevLastIndex == 0 ? 0 : this.node_state.log.get(prevLastIndex - 1).term;
            /* TODO: Here the last two params (0, 0) are for (prevLogIndex, prevLogTerm); Used Just for Now */
                AppendEntriesArgs request_args = new AppendEntriesArgs(node_state.currentTerm, this.id, prevLastIndex, prevLastTerm, logs, node_state.commitIndex);

                Lock.unlock();
                /* Open an AppendEntriesThread for each RPC call
             * Because using sendMessage to requestVote will block until there is a reply sent back.
             * */
                AppendEntriesThread thread = new AppendEntriesThread(this,this.id,i, request_args);
                thread.start();
//                System.out.println(System.currentTimeMillis() + " Node" + this.getId() + " Sent HeartBeat RPC to Node" + i + " " + prevLastIndex + " " + prevLastTerm + " " + logs.size());
            }
        }
    }
    public void requestVote() {
////        currentTerm += 1;
//        byte[] body = BytesUtil.serialize(new RequestVoteArgs(currentTerm, id, commitIndex, logs[commitIndex-1].getTerm()));
//        // TODO: Boradcast by a Loop or sth ; Specify dest_addr
//        Message reqMsg = new Message(MessageType.RequestVoteArgs,  port, 1, body);
//
//        try {
//            Message respMsg = lib.sendMessage(reqMsg);
//            RequestVoteReply reply = (RequestVoteReply) BytesUtil.deserialize(respMsg.getBody());
//        } catch (RemoteException e) {
//            e.printStackTrace();
//        }
//        // TODO:
    }

    /**
     *  Handle requestVote RPC request
     * @param req_args requestVote RPC's args
     * @return
     */
    public RequestVoteReply requestVoteHandle(RequestVoteArgs req_args){
        RequestVoteReply req_vote_reply;
        boolean if_granted = false;

        this.Lock.lock();

        /*
            1. Reply false if term < currentTerm
         */
        if(req_args.term < this.node_state.currentTerm){
            System.out.println(System.currentTimeMillis()+" Request Vote From Node" +req_args.candidateId+" Term: "+req_args.term+" To Node "+this.id+" Term "+this.node_state.currentTerm);
            req_vote_reply = new RequestVoteReply(this.node_state.currentTerm, if_granted);

            this.if_received_RPC = true;
            this.Lock.unlock();

            return req_vote_reply;
        }
        /*
            2. If votedFor is null or candidateId,
               and
               candidate’s log is at least as up-to-date as receiver’s log, grant vote
         */
        // TODO: Here candidate's log and current node's log 's comparision regarding 'up-to-date' Hasn't Done
        /* If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower  */
        if(req_args.term > this.node_state.currentTerm){
            this.node_state.currentTerm = req_args.term;
//            System.out.print(System.currentTimeMillis() + "Node " + this.getId() +" Role From "+this.node_state.get_role());
            this.node_state.set_role(State.state.follower);
//            System.out.println(System.currentTimeMillis() + "To " + this.node_state.get_role());
            this.node_state.votedFor = null; // New Term has Began
            this.votes_count = 0;
        }
        if(this.node_state.votedFor == null || this.node_state.votedFor == req_args.candidateId){
            /*
                Up-to-Date Compare
                    I: If the logs have last entries with different terms, then the log with the later term is more up-to-date.
                    II: If the logs end with the same term, then whichever log is longer is more up-to-date.
             */
            int curLastLogIndex = this.node_state.log.peekLast() == null ? 0 :  this.node_state.log.peekLast().index;
            int curLastLogTerm = this.node_state.log.peekLast() == null ? 0 :  this.node_state.log.peekLast().term;
            if(curLastLogTerm != req_args.lastLogTerm){
                if(curLastLogTerm <= req_args.lastLogTerm){
                    /* candidate’s log is at least as up-to-date as receiver’s log, grant vote */
                    if_granted = true;
                    this.node_state.votedFor = req_args.candidateId;
                }
            }
            else{
                if(curLastLogIndex <= req_args.lastLogIndex){
                    if_granted = true;
                    this.node_state.votedFor = req_args.candidateId;
                }
            }
//            if_granted = true;
//            this.node_state.votedFor = req_args.candidateId;
        }
        req_vote_reply = new RequestVoteReply(this.node_state.currentTerm, if_granted);

        this.if_received_RPC = true;
        this.Lock.unlock();
        return req_vote_reply;
    }

    /**
     *  Handle AppendEntries RPC request
     * @param req_args AppendEntries RPC's args
     * @return
     */
    public AppendEntriesReply AppendEntriesHandle(AppendEntriesArgs req_args){
        AppendEntriesReply append_entry_reply;
        boolean if_success = false;
        /*
            1. Reply false if term < currentTerm ($5.1)
         */
        this.Lock.lock();

        if(req_args.term < this.node_state.currentTerm){
            System.out.println(System.currentTimeMillis()+" AppendEntries RPC From Node" +req_args.leaderId+" Term: "+req_args.term+" To Node "+this.id+" Term "+this.node_state.currentTerm);
            append_entry_reply = new AppendEntriesReply(this.node_state.currentTerm, if_success);

            this.if_received_RPC = true;
            this.Lock.unlock();

            return append_entry_reply;
        }


        /* If RPC request contains term T > currentTerm: set currentTerm = T, convert to follower  */
        /* For Candidate, If AppendEntries RPC received from new leader: convert to follower
         * To be Specific
         *      If the leader’s term (included in its RPC) is at least as large as the candidate’s current term,
         *      then the candidate recognizes the leader as legitimate and returns to follower state. */
        if(req_args.term > this.node_state.currentTerm || this.node_state.get_role() == State.state.candidate){
            this.node_state.currentTerm = req_args.term;
//            System.out.print(System.currentTimeMillis() + "Node " + this.getId() +" Role From "+this.node_state.get_role());
            this.node_state.set_role(State.state.follower);
//            System.out.println(System.currentTimeMillis() + "To " + this.node_state.get_role());
            this.node_state.votedFor = null; // New Term has Began
            this.votes_count = 0;
        }

        // TODO: Here Only Dead with 'HeartBeat' AppendEntries RPC call, Still 4 Rules need to be implemented
        /* 2. Consistency Check
                >the leader includes the index and term of the entry in its log that immediately precedes the new entries.
                 If the follower does not find an entry in its log with the same index and term,
                 then it refuses the new entries.
         */
//        if(req_args.entries.size() > 0){
//            System.out.println(System.currentTimeMillis()+" Node "+this.id+" Get Append RPC From, Node "+req_args.leaderId);
//        }

        int prevIndex = req_args.prevLogIndex;
        int prevTerm = req_args.prevLogTerm;
        boolean consistency_check = false;
        if(prevIndex < 0 || prevTerm < 0){ // for Debug
            System.out.println("!!!!!!!! NEGATIVE INDEX OR TERM !!!!!!!!");
        }
        if(prevIndex == 0 && prevTerm == 0){
            /* Append Entries From Beginning */
            consistency_check = true;
        }
        else{
            /* Check the Entry at certain Index */
            if(this.node_state.log.size() >= prevIndex){
                if(this.node_state.log.get(prevIndex-1).index == prevIndex && this.node_state.log.get(prevIndex-1).term == prevTerm){
                    /* Share the same entry with same index & term with Leader */
                    consistency_check = true;
                }
            }

        }
        /* 3.1 Consistency_check Fail, Refuse the new Entry */
        if(!consistency_check){
            if_success = false;
            append_entry_reply = new AppendEntriesReply(this.node_state.currentTerm, if_success);

            this.if_received_RPC = true;
            this.Lock.unlock();

            return append_entry_reply;
        }
        else{
            /* 3.2 Consistency Check Well! Replicate Certain Entries */
            /* > removes any conflicting entries in the follower’s log and
               > appends entries from the leader’s log (if any)
             */
            if_success = true;
            ArrayList<LogEntries> leader_logs = req_args.entries;
            /* For HeartBeat, Remove All UnPaired Entries */
            if(leader_logs.size() == 0){
                for(int j=this.node_state.log.size() -1; j >= prevIndex; j--){
                    this.node_state.log.remove(j);
                }
            }

            for(int i=0; i<leader_logs.size(); i++){
                LogEntries entry = leader_logs.get(i);
                if(this.node_state.log.size() < entry.index){
                    /* Append any new entries not already in the follwer's log */
                    this.node_state.log.add(entry);
                }
                else{
                    LogEntries my_entry = this.node_state.log.get(entry.index-1);
                    if(my_entry.term == entry.term){
                        /* Same Entry */
                        continue;
                    }
                    else{
                        /*
                            Existing entry conflicts with a new one (same index but different terms),
                            delete the existing entry and all that following it
                         */
                        for(int j=this.node_state.log.size() -1; j >= entry.index-1; j--){
                            this.node_state.log.remove(j);
                        }
                        this.node_state.log.add(entry);
                    }
                }

            }

            /*
                If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
             */
            if(req_args.leaderCommit > this.node_state.commitIndex){
                if(this.node_state.log.peekLast() != null){
                    int last_index = this.node_state.log.getLast().index;//leader_logs.size() == 0 ? 0 : this.node_state.log.getLast().index;
                    int update_commitIndex = Math.min(req_args.leaderCommit, last_index);
                    for(int i=this.node_state.commitIndex + 1; i <= update_commitIndex; i++){
                        LogEntries entry = this.node_state.log.get(i-1);
                         /* Apply Entry to State Machine */
                        ApplyMsg msg = new ApplyMsg(this.id, entry.index, entry.command, false, null);
                        try {
                            this.lib.applyChannel(msg);
                            System.out.println(System.currentTimeMillis()+"Node:"+this.id+" Role: "+this.node_state.get_role()+" index: "+entry.index+" term: "+entry.term+" command: "+entry.command+" Has been Committed");
                        }
                        catch (RemoteException e){
                            e.printStackTrace();
                        }
                        this.node_state.commitIndex = i;
                        this.node_state.lastApplied = i;
                    }
                }
            }
            append_entry_reply = new AppendEntriesReply(this.node_state.currentTerm, if_success);

            this.if_received_RPC = true;
            this.Lock.unlock();

//            System.out.println(System.currentTimeMillis()+" Node "+this.id+" Returned Append RPC From, Node "+req_args.leaderId);

            return append_entry_reply;
        }
        /* Ture (Success) For Now */
//        if_success = true;
//        append_entry_reply = new AppendEntriesReply(this.node_state.currentTerm, if_success);
//        return append_entry_reply;
    }
    /*
     *call back.
     * Client request command to be executed --- Start Agreement on a new Entry
     *      1. The leader appends the command to its log as a new entry,
     *      2. Then issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry.
     */
    @Override
    public StartReply start(int command) {
        /* This Node has to be the leader */
        this.Lock.lock();

        StartReply reply = null;
        if(this.node_state.get_role() != State.state.leader){
            reply = new StartReply(-1, -1, false);

            this.Lock.unlock();

            return reply;
        }
        System.out.println(System.currentTimeMillis()+"Node "+this.id+" Client Submit Commad "+command);
        /* For The Leader
         * 1. Initialize
         *      'nextIndex'<index of the next log entry to send to that server (initialized to leader last log index + 1)
         *       and
         *      'matchIndex'<index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)>
         *           */
        int prevLastIndex = this.node_state.log.peekLast() == null ? 0 : this.node_state.log.peekLast().index;
        int prevLastTerm = this.node_state.log.peekLast() == null ? 0 : this.node_state.log.peekLast().term;
        int lastLogIndex = prevLastIndex + 1;

        for(int i=0; i<num_peers; i++){
            this.node_state.nextIndex[i] = prevLastIndex + 1;
            this.node_state.matchIndex[i] = 0;
        }

        /* 2. Command received from client: append entry to local log, */
        LogEntries cur_entry = new LogEntries(command, prevLastIndex+1, this.node_state.currentTerm);
        this.node_state.log.add(cur_entry);
        this.node_state.matchIndex[this.id] = prevLastIndex+1; //Update it for itself

        this.Lock.unlock();

        new ReplicateThread(this, lastLogIndex).start();

        reply = new StartReply(lastLogIndex, this.node_state.currentTerm, true);
        return reply;

        /* 3. Issues AppendEntries RPCs in parallel to each of the other servers to replicate the entry
              leader retries AppendEntries RPCs indefinitely (even after it has responded to the client)
              until all followers eventually store all log-entries
         */

//        while (true){
//            /* send AppendEntries RPCs in parallel */
//            for(int i=0; i<num_peers; i++){
//                if(i == this.id)continue;
//                /* If last logIndex ≥ nextIndex for a follower i:
//                   send AppendEntries RPC with log entries starting at nextIndex[i]
//                 */
//                this.Lock.lock();
//
//                /* Make Sure Current Raft Node is still Leader
//                   < Leader Send RPC!!!>
//                 */
//                if(this.node_state.get_role() != State.state.leader){
//
//                    this.Lock.unlock();
//
//                    return new StartReply(-1, -1, false);
//                }
//
//                if(lastLogIndex >= this.node_state.nextIndex[i]){
//                    prevLastIndex = this.node_state.nextIndex[i] - 1;
//                    prevLastTerm = prevLastIndex == 0 ? 0 : this.node_state.log.get(prevLastIndex-1).term;
//                    ArrayList<LogEntries> entries = logStartFrom(this.node_state.log, this.node_state.nextIndex[i]);
//                    AppendEntriesArgs req_args = new AppendEntriesArgs(this.node_state.currentTerm, this.id, prevLastIndex, prevLastTerm, entries, this.node_state.commitIndex);
//                    /* Start a Thread handling this RPC call */
//                    AppendEntriesThread thread = new AppendEntriesThread(this,this.id,i, req_args);
//                    thread.start();
//                    System.out.println(System.currentTimeMillis()+" Node"+this.getId()+ " Sent AppendEntries RPC to Node"+i+" "+prevLastIndex+" "+prevLastTerm+" "+entries.size());
//                }
//
//                this.Lock.unlock();
//
//            }
//            /* Wait for 50ms for all RPC call Threads to finish*/
//            long t0 = System.currentTimeMillis();
//            while( (System.currentTimeMillis() - t0 ) < 50 ){
//
//            }
//            /*
//                If there exists an N such that N > commitIndex,
//                a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm:
//                        set commitIndex = N (§5.3, §5.4).
//                TODO: Currently for Simplicity, take 'N = lastLogIndex
//             */
//
////            this.Lock.lock();
//
//            for(int N = this.node_state.commitIndex+1; N <= lastLogIndex; N++){
////                int N = lastLogIndex;
////                if(N > this.node_state.commitIndex){
//                    int match_count = 0;
//                    for(int i=0; i<num_peers; i++){
//                        if(this.node_state.matchIndex[i] >= N){
//                            match_count ++;
//                        }
//                    }
//                    /*
//                        Only log entries from the leader’s current term are committed by counting replicas;
//                        Once an entry from the current term has been committed in this way,
//                        Then all prior entries are committed indirectly
//                     */
//                    if(match_count > this.majority  && (this.node_state.log.get(N-1).term == this.node_state.currentTerm)){
//                    /* Apply Entry to State Machine */
//                        for(int k=this.node_state.commitIndex+1; k<=N; k++) {
//                            ApplyMsg msg = new ApplyMsg(this.id, k, this.node_state.log.get(k - 1).command, false, null);
//                            try {
//                                this.lib.applyChannel(msg);
//                                System.out.println(System.currentTimeMillis() + "Node:" + this.id + " Role: " + this.node_state.get_role() + " index: " + this.node_state.log.get(k - 1).index + " term: " + this.node_state.log.get(k - 1).term + " command: " + this.node_state.log.get(k - 1).command + " Has been Committed");
//                            } catch (RemoteException e) {
//                                e.printStackTrace();
//                            }
//                        }
//                            this.node_state.commitIndex = N;
//                            this.node_state.lastApplied = N;
////                            reply = new StartReply(lastLogIndex, this.node_state.currentTerm, true);
////                            return reply;
//                    }
//
//            }

//            int cur_command_Count = 0;
//            for(int t=0; t<num_peers; t++){
//                if(this.node_state.matchIndex[t] >= lastLogIndex){
//                    cur_command_Count ++;
//                }
//            }
//
//            if(cur_command_Count == num_peers){
//                reply = new StartReply(lastLogIndex, this.node_state.currentTerm, true);
//                return reply;
//            }

//            this.Lock.unlock();

//        }

//        return null;
    }

    @Override
    public GetStateReply getState() {
        /* return This RaftNode's state to Controller */
        boolean if_leader = this.node_state.get_role() == State.state.leader;

        return new GetStateReply(this.node_state.currentTerm, if_leader);
    }

    @Override
    public Message deliverMessage(Message message) {
        /* The Message delivered to this RaftNode(Destination Node for another RaftNode)
                1. Upack the Message
                2. Get the Request
                3. Process the Request
                4. Return the Reply wrapped in 'Message'
         */
//        System.out.println("This is Node: "+this.id);
//        System.out.println("Message Type: "+message.getType());
//        System.out.println("From Node "+message.getSrc()+" To Node: "+message.getDest());
        Message respond_message = null;
        MessageType type = message.getType();
        int src_id = message.getSrc();
        int dest_id = message.getDest(); // This RaftNode's ID
        if(type == MessageType.RequestVoteArgs){
            /* Request For Vote */
            RequestVoteArgs req_args = (RequestVoteArgs) BytesUtil.deserialize(message.getBody());
            RequestVoteReply reply = requestVoteHandle(req_args);
            byte[] payload = BytesUtil.serialize(reply);
            respond_message = new Message(MessageType.RequestVoteReply, dest_id, src_id, payload);
//            this.Lock.lock();
//            this.if_received_RPC = true;
//            this.Lock.unlock();
        }
        else if(type == MessageType.AppendEntriesArgs){
//            System.out.println(System.currentTimeMillis()+" Node "+this.id+" HeartBeat From "+src_id);
            /* request For Append Entries */
            AppendEntriesArgs append_args = (AppendEntriesArgs) BytesUtil.deserialize(message.getBody());
            AppendEntriesReply reply = AppendEntriesHandle(append_args);
            byte[] payload = BytesUtil.serialize(reply);
            respond_message = new Message(MessageType.AppendEntriesReply, dest_id, src_id, payload);
//            this.Lock.lock();
//            this.if_received_RPC = true;
//            this.Lock.unlock();
        }
        return respond_message;
    }

    /*
        Accessory Functions
     */

    /**
     * Get another random election_timeout
     */
    public void reset_election_timer(){
        /* Generate Timeout in range [election_timeout_lo, election_timeout_hi] */
//        this.election_timeout = rand.nextInt() % (election_timeout_hi - election_timeout_lo) + election_timeout_lo;
        this.election_timeout = rand.nextInt(election_timeout_lo) + (election_timeout_hi - election_timeout_lo);
    }

    /**
     * Entry logs start from certain index
     */
    public ArrayList<LogEntries> logStartFrom(List<LogEntries> list, int start_index){
        ArrayList<LogEntries> res = new ArrayList<LogEntries>();
        int n = list.size();
        if(n < start_index)return res;
        for(int i=start_index-1; i<n; i++){
            res.add(list.get(i));
        }

        return res;
    }

    /**
     * Remove log Entries start from certain index
     */

    /**
     *
     * @return RaftNode Id(Address) for this Raft Server
     */
    public int getId(){
        return this.id;
    }



    //main function
    public static void main(String args[]) throws Exception {
        if (args.length != 3) throw new Exception("Need 2 args: <port> <id> <num_peers>");
        //new usernode
        RaftNode UN = new RaftNode(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    }

}
