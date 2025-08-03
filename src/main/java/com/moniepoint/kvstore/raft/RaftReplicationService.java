package com.moniepoint.kvstore.raft;

import com.moniepoint.kvstore.model.KeyValue;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Raft consensus service for distributed replication.
 * Implements the Raft consensus algorithm for leader election and log replication.
 */
@Service
public class RaftReplicationService {

    // Raft node states
    public enum NodeState {
        FOLLOWER, CANDIDATE, LEADER
    }

    // Raft node information
    private final String nodeId;
    private final AtomicReference<NodeState> state;
    private final AtomicLong currentTerm;
    private final AtomicLong votedFor;
    private final AtomicLong lastLogIndex;
    private final AtomicLong lastLogTerm;
    private final AtomicLong commitIndex;
    private final AtomicLong lastApplied;

    // Leader-specific state
    private final ConcurrentHashMap<String, Long> nextIndex;
    private final ConcurrentHashMap<String, Long> matchIndex;

    // Election timeout and heartbeat
    private final long electionTimeout;
    private final long heartbeatInterval;
    private volatile long lastHeartbeatTime;

    public RaftReplicationService() {
        this.nodeId = generateNodeId();
        this.state = new AtomicReference<>(NodeState.FOLLOWER);
        this.currentTerm = new AtomicLong(0);
        this.votedFor = new AtomicLong(-1);
        this.lastLogIndex = new AtomicLong(0);
        this.lastLogTerm = new AtomicLong(0);
        this.commitIndex = new AtomicLong(0);
        this.lastApplied = new AtomicLong(0);
        
        this.nextIndex = new ConcurrentHashMap<>();
        this.matchIndex = new ConcurrentHashMap<>();
        
        this.electionTimeout = 5000; // 5 seconds
        this.heartbeatInterval = 1000; // 1 second
        this.lastHeartbeatTime = System.currentTimeMillis();
    }

    /**
     * Start the Raft node.
     */
    public void start() {
        // Start election timer
        startElectionTimer();
        
        // Start heartbeat timer if leader
        if (state.get() == NodeState.LEADER) {
            startHeartbeatTimer();
        }
    }

    /**
     * Handle client request (only if leader).
     */
    public boolean handleClientRequest(String operation, String key, String value, long ttl) {
        if (state.get() != NodeState.LEADER) {
            return false; // Redirect to leader
        }

        // Append to log
        long logIndex = appendToLog(operation, key, value, ttl);
        
        // Replicate to followers
        return replicateToFollowers(logIndex);
    }

    /**
     * Handle AppendEntries RPC from leader.
     */
    public AppendEntriesResponse handleAppendEntries(AppendEntriesRequest request) {
        if (request.getTerm() < currentTerm.get()) {
            return new AppendEntriesResponse(currentTerm.get(), false);
        }

        // Update term if received higher term
        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
            state.set(NodeState.FOLLOWER);
            votedFor.set(-1);
        }

        // Reset election timer
        lastHeartbeatTime = System.currentTimeMillis();

        // Handle heartbeat
        if (request.getEntries().isEmpty()) {
            return new AppendEntriesResponse(currentTerm.get(), true);
        }

        // Handle log entries
        if (request.getPrevLogIndex() > lastLogIndex.get() ||
            (request.getPrevLogIndex() > 0 && request.getPrevLogTerm() != getLogTerm(request.getPrevLogIndex()))) {
            return new AppendEntriesResponse(currentTerm.get(), false);
        }

        // Append new entries
        for (LogEntry entry : request.getEntries()) {
            appendLogEntry(entry);
        }

        // Update commit index
        if (request.getLeaderCommit() > commitIndex.get()) {
            commitIndex.set(Math.min(request.getLeaderCommit(), lastLogIndex.get()));
            applyCommittedEntries();
        }

        return new AppendEntriesResponse(currentTerm.get(), true);
    }

    /**
     * Handle RequestVote RPC from candidates.
     */
    public RequestVoteResponse handleRequestVote(RequestVoteRequest request) {
        if (request.getTerm() < currentTerm.get()) {
            return new RequestVoteResponse(currentTerm.get(), false);
        }

        if (request.getTerm() > currentTerm.get()) {
            currentTerm.set(request.getTerm());
            state.set(NodeState.FOLLOWER);
            votedFor.set(-1);
        }

        if (votedFor.get() == -1 || votedFor.get() == Long.parseLong(request.getCandidateId())) {
            if (request.getLastLogIndex() >= lastLogIndex.get() &&
                request.getLastLogTerm() >= lastLogTerm.get()) {
                votedFor.set(Long.parseLong(request.getCandidateId()));
                lastHeartbeatTime = System.currentTimeMillis();
                return new RequestVoteResponse(currentTerm.get(), true);
            }
        }

        return new RequestVoteResponse(currentTerm.get(), false);
    }

    /**
     * Start election process.
     */
    private void startElection() {
        currentTerm.incrementAndGet();
        state.set(NodeState.CANDIDATE);
        votedFor.set(Long.parseLong(nodeId));
        lastHeartbeatTime = System.currentTimeMillis();

        // Request votes from other nodes
        requestVotes();
    }

    /**
     * Request votes from other nodes.
     */
    private void requestVotes() {
        // This would send RequestVote RPCs to all other nodes
        // For now, just a placeholder
    }

    /**
     * Start election timer.
     */
    private void startElectionTimer() {
        Thread electionTimer = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(electionTimeout);
                    if (System.currentTimeMillis() - lastHeartbeatTime > electionTimeout) {
                        startElection();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        electionTimer.setDaemon(true);
        electionTimer.start();
    }

    /**
     * Start heartbeat timer.
     */
    private void startHeartbeatTimer() {
        Thread heartbeatTimer = new Thread(() -> {
            while (state.get() == NodeState.LEADER) {
                try {
                    Thread.sleep(heartbeatInterval);
                    sendHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
        heartbeatTimer.setDaemon(true);
        heartbeatTimer.start();
    }

    /**
     * Send heartbeat to followers.
     */
    private void sendHeartbeat() {
        // This would send AppendEntries RPCs to all followers
        // For now, just a placeholder
    }

    /**
     * Append entry to log.
     */
    private long appendToLog(String operation, String key, String value, long ttl) {
        long index = lastLogIndex.incrementAndGet();
        LogEntry entry = new LogEntry(currentTerm.get(), index, operation, key, value, ttl);
        // Store in log (implementation would use persistent storage)
        lastLogTerm.set(currentTerm.get());
        return index;
    }

    /**
     * Replicate to followers.
     */
    private boolean replicateToFollowers(long logIndex) {
        // This would send AppendEntries RPCs to followers
        // For now, just a placeholder
        return true;
    }

    /**
     * Get log term at index.
     */
    private long getLogTerm(long index) {
        // This would retrieve from persistent log
        // For now, just a placeholder
        return 0;
    }

    /**
     * Append log entry.
     */
    private void appendLogEntry(LogEntry entry) {
        // This would append to persistent log
        // For now, just a placeholder
        lastLogIndex.set(entry.getIndex());
        lastLogTerm.set(entry.getTerm());
    }

    /**
     * Apply committed entries.
     */
    private void applyCommittedEntries() {
        // This would apply committed entries to state machine
        // For now, just a placeholder
        lastApplied.set(commitIndex.get());
    }

    /**
     * Generate unique node ID.
     */
    private String generateNodeId() {
        return String.valueOf(System.currentTimeMillis() % 10000);
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public NodeState getState() { return state.get(); }
    public long getCurrentTerm() { return currentTerm.get(); }
    public long getLastLogIndex() { return lastLogIndex.get(); }
    public long getLastLogTerm() { return lastLogTerm.get(); }

    // Raft message classes
    public static class LogEntry {
        private final long term;
        private final long index;
        private final String operation;
        private final String key;
        private final String value;
        private final long ttl;

        public LogEntry(long term, long index, String operation, String key, String value, long ttl) {
            this.term = term;
            this.index = index;
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.ttl = ttl;
        }

        // Getters
        public long getTerm() { return term; }
        public long getIndex() { return index; }
        public String getOperation() { return operation; }
        public String getKey() { return key; }
        public String getValue() { return value; }
        public long getTtl() { return ttl; }
    }

    public static class AppendEntriesRequest {
        private final long term;
        private final String leaderId;
        private final long prevLogIndex;
        private final long prevLogTerm;
        private final List<LogEntry> entries;
        private final long leaderCommit;

        public AppendEntriesRequest(long term, String leaderId, long prevLogIndex, 
                                 long prevLogTerm, List<LogEntry> entries, long leaderCommit) {
            this.term = term;
            this.leaderId = leaderId;
            this.prevLogIndex = prevLogIndex;
            this.prevLogTerm = prevLogTerm;
            this.entries = entries;
            this.leaderCommit = leaderCommit;
        }

        // Getters
        public long getTerm() { return term; }
        public String getLeaderId() { return leaderId; }
        public long getPrevLogIndex() { return prevLogIndex; }
        public long getPrevLogTerm() { return prevLogTerm; }
        public List<LogEntry> getEntries() { return entries; }
        public long getLeaderCommit() { return leaderCommit; }
    }

    public static class AppendEntriesResponse {
        private final long term;
        private final boolean success;

        public AppendEntriesResponse(long term, boolean success) {
            this.term = term;
            this.success = success;
        }

        // Getters
        public long getTerm() { return term; }
        public boolean isSuccess() { return success; }
    }

    public static class RequestVoteRequest {
        private final long term;
        private final String candidateId;
        private final long lastLogIndex;
        private final long lastLogTerm;

        public RequestVoteRequest(long term, String candidateId, long lastLogIndex, long lastLogTerm) {
            this.term = term;
            this.candidateId = candidateId;
            this.lastLogIndex = lastLogIndex;
            this.lastLogTerm = lastLogTerm;
        }

        // Getters
        public long getTerm() { return term; }
        public String getCandidateId() { return candidateId; }
        public long getLastLogIndex() { return lastLogIndex; }
        public long getLastLogTerm() { return lastLogTerm; }
    }

    public static class RequestVoteResponse {
        private final long term;
        private final boolean voteGranted;

        public RequestVoteResponse(long term, boolean voteGranted) {
            this.term = term;
            this.voteGranted = voteGranted;
        }

        // Getters
        public long getTerm() { return term; }
        public boolean isVoteGranted() { return voteGranted; }
    }
} 