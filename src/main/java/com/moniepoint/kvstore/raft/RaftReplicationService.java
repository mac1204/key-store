package com.moniepoint.kvstore.raft;

import com.moniepoint.kvstore.cluster.ClusterManager;
import com.moniepoint.kvstore.cluster.HashRing;
import com.moniepoint.kvstore.network.NetworkManager;
import com.moniepoint.kvstore.service.KeyValueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Enhanced Raft consensus service with actual replication and network communication.
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

    // Persistent log storage
    private final List<LogEntry> log;
    private final String logFile;

    // Cluster management and network
    private final ClusterManager clusterManager;
    private final NetworkManager networkManager;

    @Autowired
    public RaftReplicationService(ClusterManager clusterManager, NetworkManager networkManager) {
        this.clusterManager = clusterManager;
        this.networkManager = networkManager;
        
        // Use a temporary node ID until cluster is initialized
        HashRing.ClusterNode self = clusterManager.getSelf();
        this.nodeId = self != null ? self.getId() : "raft-node-" + System.currentTimeMillis();
        
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
        
        this.log = new ArrayList<>();
        this.logFile = "data/raft-log-" + nodeId + ".dat";
        
        // Load existing log
        loadLog();
    }

    /**
     * Start the Raft node.
     */
    public void start() {
        // Check if Raft is enabled
        if (!clusterManager.shouldEnableRaft()) {
            System.out.println("ℹ️ Raft consensus disabled in configuration");
            return;
        }

        // Check if this is a single-node setup
        if (clusterManager.isSingleNodeSetup()) {
            System.out.println("ℹ️ Single-node setup detected, skipping Raft elections");
            state.set(NodeState.LEADER);
            System.out.println("👑 Node " + nodeId + " automatically became leader (single-node mode)");
            return;
        }

        // Check if auto-election is enabled
        if (!clusterManager.shouldEnableAutoElection()) {
            System.out.println("ℹ️ Auto-election disabled, staying in FOLLOWER state");
            return;
        }

        // Use smart configuration from ClusterManager
        Map<String, Object> raftConfig = clusterManager.getSmartRaftConfig();
        System.out.println("⚙️ Raft Configuration: " + raftConfig);

        // Start election timer for multi-node setup
        startElectionTimer();
        
        // Start heartbeat timer if leader
        if (state.get() == NodeState.LEADER) {
            startHeartbeatTimer();
        }
        
        System.out.println("🚀 Raft node " + nodeId + " started in " + state.get() + " state");
    }

    /**
     * Handle client request (only if leader).
     */
    public boolean handleClientRequest(String operation, String key, String value) {
        if (state.get() != NodeState.LEADER) {
            System.out.println("Node " + nodeId + " is not leader, redirecting request");
            return false; // Redirect to leader
        }

        // Append to log
        long logIndex = appendToLog(operation, key, value);
        
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
        System.out.println("Starting election for term " + (currentTerm.get() + 1));
        
        // Increment term
        currentTerm.incrementAndGet();
        
        // Change to candidate state
        state.set(NodeState.CANDIDATE);
        
        // Vote for self (use hash of nodeId for numeric value)
        votedFor.set((long) nodeId.hashCode());
        
        // Request votes from other nodes
        requestVotes();
    }

    /**
     * Request votes from other nodes.
     */
    private void requestVotes() {
        // For single-node setup, automatically become leader
        if (clusterManager.isSingleNodeSetup()) {
            becomeLeader();
            return;
        }

        // Get only online nodes for voting
        List<HashRing.ClusterNode> onlineNodes = clusterManager.getOnlineNodes();
        if (onlineNodes.isEmpty()) {
            System.out.println("⚠️ No online nodes available for voting, staying in CANDIDATE state");
            return;
        }

        System.out.println("🗳️ Requesting votes from " + onlineNodes.size() + " online nodes...");
        
        AtomicInteger votesReceived = new AtomicInteger(1); // Vote for self
        AtomicInteger totalVotes = new AtomicInteger(onlineNodes.size() + 1); // +1 for self
        
        for (HashRing.ClusterNode node : onlineNodes) {
            if (node.getId().equals(nodeId)) {
                continue; // Skip self
            }
            
            networkManager.sendVoteRequest(node, nodeId, currentTerm.get())
                .thenAccept(voteGranted -> {
                    if (voteGranted) {
                        int votes = votesReceived.incrementAndGet();
                        System.out.println("✅ Received vote from " + node.getId() + " (total: " + votes + "/" + totalVotes.get() + ")");
                        
                        // Check if we have majority
                        if (votes > totalVotes.get() / 2) {
                            becomeLeader();
                        }
                    } else {
                        System.out.println("❌ Vote denied by " + node.getId());
                    }
                })
                .exceptionally(throwable -> {
                    System.out.println("❌ Vote request failed for " + node.getId() + ": " + throwable.getMessage());
                    return null;
                });
        }
        
        // If no other nodes are available, become leader automatically
        if (onlineNodes.size() == 1 && onlineNodes.get(0).getId().equals(nodeId)) {
            System.out.println("👑 No other nodes available, becoming leader automatically");
            becomeLeader();
        }
    }

    /**
     * Become leader and start heartbeat.
     */
    private void becomeLeader() {
        state.set(NodeState.LEADER);
        System.out.println("Node " + nodeId + " became leader for term " + currentTerm.get());
        
        // Initialize leader state
        List<HashRing.ClusterNode> nodes = clusterManager.getOnlineNodes();
        for (HashRing.ClusterNode node : nodes) {
            if (!node.getId().equals(nodeId)) {
                nextIndex.put(node.getId(), lastLogIndex.get() + 1);
                matchIndex.put(node.getId(), 0L);
            }
        }
        
        // Start heartbeat timer
        startHeartbeatTimer();
    }

    /**
     * Start election timer.
     */
    private void startElectionTimer() {
        // Use configured election timeout
        long timeout = clusterManager.getRaftConfig().getElectionTimeout();
        
        CompletableFuture.runAsync(() -> {
            while (state.get() != NodeState.LEADER) {
                try {
                    Thread.sleep(timeout);
                    
                    // Check if we should start election
                    if (System.currentTimeMillis() - lastHeartbeatTime > timeout) {
                        startElection();
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * Start heartbeat timer.
     */
    private void startHeartbeatTimer() {
        // Use configured heartbeat interval
        long interval = clusterManager.getRaftConfig().getHeartbeatInterval();
        
        CompletableFuture.runAsync(() -> {
            while (state.get() == NodeState.LEADER) {
                try {
                    Thread.sleep(interval);
                    sendHeartbeat();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });
    }

    /**
     * Send heartbeat to followers.
     */
    private void sendHeartbeat() {
        List<HashRing.ClusterNode> nodes = clusterManager.getOnlineNodes();
        
        for (HashRing.ClusterNode node : nodes) {
            if (!node.getId().equals(nodeId)) {
                try {
                    // Send empty AppendEntries as heartbeat
                    String entries = "[]"; // Empty entries for heartbeat
                    networkManager.sendAppendEntries(node, nodeId, currentTerm.get(), entries)
                        .thenAccept(success -> {
                            if (!success) {
                                // Decrement nextIndex for retry
                                nextIndex.put(node.getId(), Math.max(1, nextIndex.get(node.getId()) - 1));
                            }
                        });
                } catch (Exception e) {
                    System.out.println("Failed to send heartbeat to " + node.getId() + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Append entry to log.
     */
    private long appendToLog(String operation, String key, String value) {
        long index = lastLogIndex.incrementAndGet();
        LogEntry entry = new LogEntry(currentTerm.get(), index, operation, key, value);
        
        // Add to in-memory log
        log.add(entry);
        
        // Persist to disk
        persistLog();
        
        lastLogTerm.set(currentTerm.get());
        return index;
    }

    /**
     * Replicate to followers.
     */
    private boolean replicateToFollowers(long logIndex) {
        List<HashRing.ClusterNode> nodes = clusterManager.getOnlineNodes();
        int replicatedCount = 1; // Leader has it
        
        List<CompletableFuture<Boolean>> replicationRequests = new ArrayList<>();
        
        for (HashRing.ClusterNode node : nodes) {
            if (!node.getId().equals(nodeId)) {
                try {
                    // Get entries to replicate
                    List<LogEntry> entries = getEntriesToReplicate(node.getId());
                    String entriesJson = serializeEntries(entries);
                    
                    CompletableFuture<Boolean> replicationRequest = networkManager.sendAppendEntries(
                        node, nodeId, currentTerm.get(), entriesJson
                    ).thenApply(success -> {
                        if (success) {
                            nextIndex.put(node.getId(), nextIndex.get(node.getId()) + entries.size());
                            matchIndex.put(node.getId(), logIndex);
                            return true;
                        } else {
                            // Decrement nextIndex for retry
                            nextIndex.put(node.getId(), Math.max(1, nextIndex.get(node.getId()) - 1));
                            return false;
                        }
                    });
                    replicationRequests.add(replicationRequest);
                } catch (Exception e) {
                    System.out.println("Failed to replicate to " + node.getId() + ": " + e.getMessage());
                }
            }
        }
        
        // Wait for replication to complete
        CompletableFuture.allOf(replicationRequests.toArray(new CompletableFuture[0]))
            .thenRun(() -> {
                long successfulReplications = replicationRequests.stream()
                    .mapToLong(future -> future.join() ? 1 : 0)
                    .sum() + 1; // +1 for leader
                
                if (successfulReplications > nodes.size() / 2) {
                    commitLog(logIndex);
                }
            });
        
        return true; // Return true for now, actual result will be determined asynchronously
    }

    /**
     * Serialize log entries to JSON.
     */
    private String serializeEntries(List<LogEntry> entries) {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < entries.size(); i++) {
            LogEntry entry = entries.get(i);
            json.append(String.format(
                "{\"term\":%d,\"index\":%d,\"operation\":\"%s\",\"key\":\"%s\",\"value\":\"%s\"}",
                entry.getTerm(), entry.getIndex(), entry.getOperation(), entry.getKey(), entry.getValue()
            ));
            if (i < entries.size() - 1) {
                json.append(",");
            }
        }
        json.append("]");
        return json.toString();
    }

    /**
     * Get entries to replicate to a specific node.
     */
    private List<LogEntry> getEntriesToReplicate(String nodeId) {
        long nextIdx = nextIndex.getOrDefault(nodeId, 1L);
        List<LogEntry> entries = new ArrayList<>();
        
        for (int i = (int) nextIdx; i <= lastLogIndex.get(); i++) {
            if (i < log.size()) {
                entries.add(log.get(i));
            }
        }
        
        return entries;
    }

    /**
     * Commit log entries.
     */
    private void commitLog(long logIndex) {
        commitIndex.set(logIndex);
        applyCommittedEntries();
        System.out.println("Committed log entry " + logIndex);
    }

    /**
     * Get log term at index.
     */
    private long getLogTerm(long index) {
        if (index < log.size()) {
            return log.get((int) index).getTerm();
        }
        return 0;
    }

    /**
     * Append log entry.
     */
    private void appendLogEntry(LogEntry entry) {
        // Ensure log has enough space
        while (log.size() <= entry.getIndex()) {
            log.add(null);
        }
        
        log.set((int) entry.getIndex(), entry);
        lastLogIndex.set(entry.getIndex());
        lastLogTerm.set(entry.getTerm());
        
        // Persist to disk
        persistLog();
    }

    /**
     * Apply committed entries.
     */
    private void applyCommittedEntries() {
        while (lastApplied.get() < commitIndex.get()) {
            lastApplied.incrementAndGet();
            LogEntry entry = log.get((int) lastApplied.get());
            
            if (entry != null) {
                // Apply to state machine
                applyToStateMachine(entry);
            }
        }
    }

    /**
     * Apply entry to state machine.
     */
    private void applyToStateMachine(LogEntry entry) {
        try {
            // Log the operation instead of directly applying to KeyValueService
            // This avoids circular dependency
            System.out.println("Raft: Applying " + entry.getOperation() + " for key " + entry.getKey() + 
                " with value: " + entry.getValue());
            
            // In a real implementation, this would trigger an event or use a different mechanism
            // to apply the operation to the state machine without circular dependency
        } catch (Exception e) {
            System.err.println("Failed to apply operation " + entry.getOperation() + 
                " for key " + entry.getKey() + ": " + e.getMessage());
        }
    }

    /**
     * Persist log to disk.
     */
    private void persistLog() {
        try {
            File file = new File(logFile);
            file.getParentFile().mkdirs();
            
            try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(file))) {
                oos.writeObject(log);
            }
        } catch (IOException e) {
            System.err.println("Failed to persist log: " + e.getMessage());
        }
    }

    /**
     * Load log from disk.
     */
    @SuppressWarnings("unchecked")
    private void loadLog() {
        try {
            File file = new File(logFile);
            if (file.exists()) {
                try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(file))) {
                    List<LogEntry> loadedLog = (List<LogEntry>) ois.readObject();
                    log.clear();
                    log.addAll(loadedLog);
                    
                    if (!log.isEmpty()) {
                        lastLogIndex.set(log.size() - 1);
                        lastLogTerm.set(log.get(log.size() - 1).getTerm());
                    }
                }
            }
        } catch (IOException | ClassNotFoundException e) {
            System.err.println("Failed to load log: " + e.getMessage());
        }
    }

    // Getters
    public String getNodeId() { return nodeId; }
    public NodeState getState() { return state.get(); }
    public long getCurrentTerm() { return currentTerm.get(); }
    public long getLastLogIndex() { return lastLogIndex.get(); }
    public long getLastLogTerm() { return lastLogTerm.get(); }

    // Raft message classes (existing code remains the same)
    public static class LogEntry implements Serializable {
        private final long term;
        private final long index;
        private final String operation;
        private final String key;
        private final String value;

        public LogEntry(long term, long index, String operation, String key, String value) {
            this.term = term;
            this.index = index;
            this.operation = operation;
            this.key = key;
            this.value = value;
        }

        public long getTerm() { return term; }
        public long getIndex() { return index; }
        public String getOperation() { return operation; }
        public String getKey() { return key; }
        public String getValue() { return value; }
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

        public long getTerm() { return term; }
        public boolean isVoteGranted() { return voteGranted; }
    }
} 