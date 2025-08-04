package com.moniepoint.kvstore.cluster;

import com.moniepoint.kvstore.network.NetworkManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * Configuration class for cluster nodes.
 */
@Component
@ConfigurationProperties(prefix = "kvstore.cluster")
class ClusterConfig {
    private List<ClusterNodeConfig> nodes = new ArrayList<>();
    private boolean enableNetworkDiscovery = false;
    private boolean autoElection = true;
    private RaftConfig raft = new RaftConfig();

    public List<ClusterNodeConfig> getNodes() { return nodes; }
    public void setNodes(List<ClusterNodeConfig> nodes) { this.nodes = nodes; }
    public boolean isEnableNetworkDiscovery() { return enableNetworkDiscovery; }
    public void setEnableNetworkDiscovery(boolean enableNetworkDiscovery) { this.enableNetworkDiscovery = enableNetworkDiscovery; }
    public boolean isAutoElection() { return autoElection; }
    public void setAutoElection(boolean autoElection) { this.autoElection = autoElection; }
    public RaftConfig getRaft() { return raft; }
    public void setRaft(RaftConfig raft) { this.raft = raft; }

    public static class ClusterNodeConfig {
        private String id;
        private String host;
        private int port;
        private int grpcPort;
        private int raftPort;

        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
        public String getHost() { return host; }
        public void setHost(String host) { this.host = host; }
        public int getPort() { return port; }
        public void setPort(int port) { this.port = port; }
        public int getGrpcPort() { return grpcPort; }
        public void setGrpcPort(int grpcPort) { this.grpcPort = grpcPort; }
        public int getRaftPort() { return raftPort; }
        public void setRaftPort(int raftPort) { this.raftPort = raftPort; }
    }

    public static class RaftConfig {
        private boolean enabled = true;
        private long electionTimeout = 5000;
        private long heartbeatInterval = 1000;
        private LogConfig log = new LogConfig();

        public boolean isEnabled() { return enabled; }
        public void setEnabled(boolean enabled) { this.enabled = enabled; }
        public long getElectionTimeout() { return electionTimeout; }
        public void setElectionTimeout(long electionTimeout) { this.electionTimeout = electionTimeout; }
        public long getHeartbeatInterval() { return heartbeatInterval; }
        public void setHeartbeatInterval(long heartbeatInterval) { this.heartbeatInterval = heartbeatInterval; }
        public LogConfig getLog() { return log; }
        public void setLog(LogConfig log) { this.log = log; }

        public static class LogConfig {
            private boolean enabled = true;
            private long syncInterval = 1000;
            private String maxSize = "1GB";

            public boolean isEnabled() { return enabled; }
            public void setEnabled(boolean enabled) { this.enabled = enabled; }
            public long getSyncInterval() { return syncInterval; }
            public void setSyncInterval(long syncInterval) { this.syncInterval = syncInterval; }
            public String getMaxSize() { return maxSize; }
            public void setMaxSize(String maxSize) { this.maxSize = maxSize; }
        }
    }
}

/**
 * Manages cluster membership, node discovery, and health monitoring.
 */
@Service
public class ClusterManager {

    @Value("${kvstore.node.id:node1}")
    private String nodeId;

    @Value("${kvstore.node.host:localhost}")
    private String nodeHost;

    @Value("${server.port:8080}")
    private int nodePort;

    @Value("${kvstore.grpc.port:9090}")
    private int grpcPort;

    @Value("${kvstore.raft.port:9091}")
    private int raftPort;

    private final HashRing hashRing;
    private final NetworkManager networkManager;
    private final Map<String, NodeHealth> nodeHealthMap;
    private final AtomicBoolean isInitialized;
    private final long heartbeatInterval;
    private final long nodeTimeout;
    private final long failoverTimeout;
    private final ClusterConfig clusterConfig;

    @Autowired
    public ClusterManager(HashRing hashRing, NetworkManager networkManager, ClusterConfig clusterConfig) {
        this.hashRing = hashRing;
        this.networkManager = networkManager;
        this.clusterConfig = clusterConfig;
        this.nodeHealthMap = new ConcurrentHashMap<>();
        this.isInitialized = new AtomicBoolean(false);
        this.heartbeatInterval = 5000; // 5 seconds
        this.nodeTimeout = 15000; // 15 seconds
        this.failoverTimeout = 30000; // 30 seconds
    }

    /**
     * Initialize cluster management.
     */
    public void initialize() {
        if (isInitialized.compareAndSet(false, true)) {
            System.out.println("🚀 Initializing Cluster Manager...");
            
            // Add self to cluster
            addSelfToCluster();
            
            // Add configured nodes from YAML
            addConfiguredNodesFromYaml();
            
            // Enable network discovery only if multiple nodes are configured
            if (clusterConfig.isEnableNetworkDiscovery() && getTotalConfiguredNodes() > 1) {
                discoverNodesViaNetwork();
            }
            
            System.out.println("✅ Cluster manager initialized with node: " + nodeId);
            System.out.println("📊 Total nodes in cluster: " + nodeHealthMap.size());
            System.out.println("🔍 Network discovery: " + (clusterConfig.isEnableNetworkDiscovery() ? "ENABLED" : "DISABLED"));
            System.out.println("🗳️ Auto-election: " + (clusterConfig.isAutoElection() ? "ENABLED" : "DISABLED"));
        }
    }

    /**
     * Add self to the cluster.
     */
    private void addSelfToCluster() {
        HashRing.ClusterNode self = new HashRing.ClusterNode(nodeId, nodeHost, nodePort, grpcPort, raftPort);
        hashRing.addNode(self);
        nodeHealthMap.put(nodeId, new NodeHealth(self, System.currentTimeMillis()));
        System.out.println("✅ Added self to cluster: " + nodeId + " at " + nodeHost + ":" + nodePort);
    }

    /**
     * Parse and add configured nodes from YAML configuration.
     */
    private void addConfiguredNodesFromYaml() {
        List<ClusterConfig.ClusterNodeConfig> configuredNodes = clusterConfig.getNodes();
        
        if (configuredNodes == null || configuredNodes.isEmpty()) {
            System.out.println("ℹ️ No configured nodes found in YAML, running in single-node mode");
            return;
        }

        System.out.println("📋 Found " + configuredNodes.size() + " configured nodes in YAML");
        
        for (ClusterConfig.ClusterNodeConfig nodeConfig : configuredNodes) {
            // Don't add self again
            if (!nodeConfig.getId().equals(nodeId)) {
                addNode(nodeConfig.getId(), nodeConfig.getHost(), nodeConfig.getPort(), 
                       nodeConfig.getGrpcPort(), nodeConfig.getRaftPort());
                System.out.println("✅ Added configured node: " + nodeConfig.getId() + " at " + 
                                nodeConfig.getHost() + ":" + nodeConfig.getPort());
            }
        }
    }

    /**
     * Get total number of configured nodes.
     */
    private int getTotalConfiguredNodes() {
        return nodeHealthMap.size();
    }

    /**
     * Check if this is a single-node setup.
     */
    public boolean isSingleNodeSetup() {
        return getTotalConfiguredNodes() <= 1;
    }

    /**
     * Check if auto-election should be enabled.
     */
    public boolean shouldEnableAutoElection() {
        return clusterConfig.isAutoElection() && !isSingleNodeSetup();
    }

    /**
     * Get Raft configuration.
     */
    public ClusterConfig.RaftConfig getRaftConfig() {
        return clusterConfig.getRaft();
    }

    /**
     * Check if Raft should be enabled.
     */
    public boolean shouldEnableRaft() {
        return clusterConfig.getRaft().isEnabled();
    }

    /**
     * Get smart Raft configuration based on node count.
     */
    public Map<String, Object> getSmartRaftConfig() {
        Map<String, Object> config = new HashMap<>();
        
        if (isSingleNodeSetup()) {
            config.put("mode", "SINGLE_NODE");
            config.put("elections", "DISABLED");
            config.put("consensus", "AUTO_LEADER");
            config.put("heartbeats", "DISABLED");
        } else {
            config.put("mode", "MULTI_NODE");
            config.put("elections", shouldEnableAutoElection() ? "ENABLED" : "DISABLED");
            config.put("consensus", "RAFT");
            config.put("heartbeats", "ENABLED");
            config.put("electionTimeout", clusterConfig.getRaft().getElectionTimeout());
            config.put("heartbeatInterval", clusterConfig.getRaft().getHeartbeatInterval());
        }
        
        config.put("logEnabled", clusterConfig.getRaft().getLog().isEnabled());
        config.put("logSyncInterval", clusterConfig.getRaft().getLog().getSyncInterval());
        config.put("logMaxSize", clusterConfig.getRaft().getLog().getMaxSize());
        
        return config;
    }

    /**
     * Discover nodes via network scanning (only for multi-node setups).
     */
    private void discoverNodesViaNetwork() {
        if (isSingleNodeSetup()) {
            System.out.println("ℹ️ Skipping network discovery for single-node setup");
            return;
        }

        System.out.println("🔍 Starting network discovery...");
        
        // Scan common ports for other nodes
        List<Integer> commonPorts = Arrays.asList(8080, 8082, 8084, 8086, 8088);
        
        for (int port : commonPorts) {
            if (port != nodePort) {
                String host = nodeHost;
                CompletableFuture.runAsync(() -> {
                    try {
                        HashRing.ClusterNode potentialNode = new HashRing.ClusterNode(
                            "discovered-" + port, host, port, port + 10, port + 11
                        );
                        
                        // Try to connect
                        networkManager.isNodeReachable(potentialNode).thenAccept(reachable -> {
                            if (reachable) {
                                addNode(potentialNode.getId(), host, port, port + 10, port + 11);
                                System.out.println("🔍 Discovered node: " + potentialNode.getId() + " at " + host + ":" + port);
                            }
                        });
                    } catch (Exception e) {
                        // Node not reachable, ignore
                    }
                });
            }
        }
    }

    /**
     * Add a node to the cluster.
     */
    public void addNode(String nodeId, String host, int port, int grpcPort, int raftPort) {
        HashRing.ClusterNode node = new HashRing.ClusterNode(nodeId, host, port, grpcPort, raftPort);
        hashRing.addNode(node);
        nodeHealthMap.put(nodeId, new NodeHealth(node, System.currentTimeMillis()));
        System.out.println("Added node to cluster: " + nodeId + " at " + host + ":" + port);
    }

    /**
     * Remove a node from the cluster.
     */
    public void removeNode(String nodeId) {
        hashRing.removeNode(nodeId);
        nodeHealthMap.remove(nodeId);
        System.out.println("Removed node from cluster: " + nodeId);
    }

    /**
     * Get the responsible node for a key.
     */
    public HashRing.ClusterNode getResponsibleNode(String key) {
        return hashRing.getResponsibleNode(key);
    }

    /**
     * Get replica nodes for a key.
     */
    public List<HashRing.ClusterNode> getReplicaNodes(String key) {
        return hashRing.getReplicaNodes(key);
    }

    /**
     * Check if this node is responsible for a key.
     */
    public boolean isResponsibleFor(String key) {
        HashRing.ClusterNode responsible = getResponsibleNode(key);
        return responsible != null && responsible.getId().equals(nodeId);
    }

    /**
     * Get all online nodes.
     */
    public List<HashRing.ClusterNode> getOnlineNodes() {
        return nodeHealthMap.values().stream()
                .filter(NodeHealth::isOnline)
                .map(NodeHealth::getNode)
                .collect(Collectors.toList());
    }

    /**
     * Get all nodes including offline ones.
     */
    public List<HashRing.ClusterNode> getAllNodes() {
        return nodeHealthMap.values().stream()
                .map(NodeHealth::getNode)
                .collect(Collectors.toList());
    }

    /**
     * Get cluster statistics.
     */
    public Map<String, Object> getClusterStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("nodeId", nodeId);
        stats.put("totalNodes", nodeHealthMap.size());
        stats.put("onlineNodes", getOnlineNodes().size());
        stats.put("hashRingStats", hashRing.getStats());
        
        // Node health information
        Map<String, Object> nodeHealth = new HashMap<>();
        for (Map.Entry<String, NodeHealth> entry : nodeHealthMap.entrySet()) {
            NodeHealth health = entry.getValue();
            Map<String, Object> healthInfo = new HashMap<>();
            healthInfo.put("state", health.getNode().getState());
            healthInfo.put("lastHeartbeat", health.getLastHeartbeat());
            healthInfo.put("isOnline", health.isOnline());
            healthInfo.put("responseTime", health.getAverageResponseTime());
            nodeHealth.put(entry.getKey(), healthInfo);
        }
        stats.put("nodeHealth", nodeHealth);
        
        return stats;
    }

    /**
     * Update node heartbeat.
     */
    public void updateHeartbeat(String nodeId) {
        NodeHealth health = nodeHealthMap.get(nodeId);
        if (health != null) {
            health.updateHeartbeat();
            health.getNode().setState(HashRing.NodeState.ONLINE);
        }
    }

    /**
     * Mark node as offline.
     */
    public void markNodeOffline(String nodeId) {
        NodeHealth health = nodeHealthMap.get(nodeId);
        if (health != null) {
            health.getNode().setState(HashRing.NodeState.OFFLINE);
            System.out.println("Node " + nodeId + " marked as offline");
        }
    }

    /**
     * Check if a node is online.
     */
    public boolean isNodeOnline(String nodeId) {
        NodeHealth health = nodeHealthMap.get(nodeId);
        return health != null && health.isOnline();
    }

    /**
     * Get node by ID.
     */
    public HashRing.ClusterNode getNode(String nodeId) {
        return hashRing.getNode(nodeId);
    }

    /**
     * Get this node's information.
     */
    public HashRing.ClusterNode getSelf() {
        return hashRing.getNode(nodeId);
    }

    /**
     * Handle node failure and trigger failover.
     */
    public void handleNodeFailure(String failedNodeId) {
        System.out.println("Handling failure of node: " + failedNodeId);
        
        // Mark node as failed
        NodeHealth health = nodeHealthMap.get(failedNodeId);
        if (health != null) {
            health.getNode().setState(HashRing.NodeState.FAILED);
        }
        
        // Trigger failover if this node was responsible for any keys
        triggerFailover(failedNodeId);
    }

    /**
     * Trigger failover for a failed node.
     */
    private void triggerFailover(String failedNodeId) {
        // Get all keys that were managed by the failed node
        List<String> affectedKeys = getKeysManagedByNode(failedNodeId);
        
        for (String key : affectedKeys) {
            // Find new responsible node
            HashRing.ClusterNode newResponsible = hashRing.getResponsibleNode(key);
            if (newResponsible != null && !newResponsible.getId().equals(failedNodeId)) {
                System.out.println("Failover: Key '" + key + "' now managed by " + newResponsible.getId());
                
                // Replicate data to new responsible node
                replicateDataToNode(key, newResponsible);
            }
        }
    }

    /**
     * Get keys managed by a specific node (simplified implementation).
     */
    private List<String> getKeysManagedByNode(String nodeId) {
        // In a real implementation, this would query the actual data store
        // For now, return an empty list
        return new ArrayList<>();
    }

    /**
     * Replicate data to a specific node.
     */
    private void replicateDataToNode(String key, HashRing.ClusterNode targetNode) {
        // In a real implementation, this would copy data from local storage to target node
        System.out.println("Replicating data for key '" + key + "' to node " + targetNode.getId());
    }

    /**
     * Scheduled task to check node health.
     */
    @Scheduled(fixedRate = 5000) // Every 5 seconds
    public void checkNodeHealth() {
        // Skip health checks for single-node setup
        if (isSingleNodeSetup()) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        
        for (Map.Entry<String, NodeHealth> entry : nodeHealthMap.entrySet()) {
            String nodeId = entry.getKey();
            NodeHealth health = entry.getValue();
            
            if (nodeId.equals(this.nodeId)) {
                // Skip self
                continue;
            }
            
            // Send heartbeat
            networkManager.sendHeartbeat(health.getNode()).thenAccept(success -> {
                if (success) {
                    updateHeartbeat(nodeId);
                } else {
                    if (currentTime - health.getLastHeartbeat() > nodeTimeout) {
                        markNodeOffline(nodeId);
                        
                        // Check if we should trigger failover
                        if (currentTime - health.getLastHeartbeat() > failoverTimeout) {
                            handleNodeFailure(nodeId);
                        }
                    }
                }
            });
        }
    }

    /**
     * Enhanced node health tracking.
     */
    private static class NodeHealth {
        private final HashRing.ClusterNode node;
        private volatile long lastHeartbeat;
        private final long timeout;
        private final List<Long> responseTimes;
        private final int maxResponseTimeHistory;

        public NodeHealth(HashRing.ClusterNode node, long lastHeartbeat) {
            this.node = node;
            this.lastHeartbeat = lastHeartbeat;
            this.timeout = 15000; // 15 seconds
            this.responseTimes = new ArrayList<>();
            this.maxResponseTimeHistory = 10;
        }

        public HashRing.ClusterNode getNode() { return node; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        
        public void updateHeartbeat() {
            this.lastHeartbeat = System.currentTimeMillis();
        }
        
        public boolean isOnline() {
            return System.currentTimeMillis() - lastHeartbeat < timeout;
        }
        
        public void addResponseTime(long responseTime) {
            responseTimes.add(responseTime);
            if (responseTimes.size() > maxResponseTimeHistory) {
                responseTimes.remove(0);
            }
        }
        
        public double getAverageResponseTime() {
            if (responseTimes.isEmpty()) {
                return 0.0;
            }
            return responseTimes.stream().mapToLong(Long::longValue).average().orElse(0.0);
        }
    }
} 