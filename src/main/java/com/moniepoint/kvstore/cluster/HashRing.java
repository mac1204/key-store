package com.moniepoint.kvstore.cluster;

import org.springframework.stereotype.Component;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Consistent Hashing implementation for distributed key-value store.
 * Provides even key distribution and fault tolerance across cluster nodes.
 */
@Component
public class HashRing {

    private final TreeMap<Integer, ClusterNode> ring;
    private final Map<String, ClusterNode> nodes;
    private final AtomicInteger virtualNodeCounter;
    private final int virtualNodesPerNode;
    private final int replicationFactor;

    public HashRing() {
        this.ring = new TreeMap<>();
        this.nodes = new ConcurrentHashMap<>();
        this.virtualNodeCounter = new AtomicInteger(0);
        this.virtualNodesPerNode = 150; // Default from config
        this.replicationFactor = 3; // Default from config
    }

    /**
     * Add a node to the hash ring.
     */
    public void addNode(ClusterNode node) {
        nodes.put(node.getId(), node);
        
        // Add virtual nodes for better distribution
        for (int i = 0; i < virtualNodesPerNode; i++) {
            String virtualNodeId = node.getId() + "-vnode-" + i;
            int hash = hash(virtualNodeId);
            ring.put(hash, node);
        }
        
        System.out.println("Added node " + node.getId() + " to hash ring with " + virtualNodesPerNode + " virtual nodes");
    }

    /**
     * Remove a node from the hash ring.
     */
    public void removeNode(String nodeId) {
        ClusterNode node = nodes.remove(nodeId);
        if (node != null) {
            // Remove all virtual nodes
            for (int i = 0; i < virtualNodesPerNode; i++) {
                String virtualNodeId = nodeId + "-vnode-" + i;
                int hash = hash(virtualNodeId);
                ring.remove(hash);
            }
            System.out.println("Removed node " + nodeId + " from hash ring");
        }
    }

    /**
     * Get the responsible node for a key.
     */
    public ClusterNode getResponsibleNode(String key) {
        if (ring.isEmpty()) {
            return null;
        }

        int hash = hash(key);
        Map.Entry<Integer, ClusterNode> entry = ring.ceilingEntry(hash);
        
        if (entry == null) {
            // Wrap around to the first entry
            entry = ring.firstEntry();
        }
        
        return entry.getValue();
    }

    /**
     * Get replica nodes for a key (for fault tolerance).
     */
    public List<ClusterNode> getReplicaNodes(String key) {
        List<ClusterNode> replicas = new ArrayList<>();
        ClusterNode primary = getResponsibleNode(key);
        
        if (primary == null) {
            return replicas;
        }

        replicas.add(primary);
        
        // Get additional replicas
        int hash = hash(key);
        Iterator<Map.Entry<Integer, ClusterNode>> iterator = ring.entrySet().iterator();
        
        // Find the starting position
        while (iterator.hasNext()) {
            Map.Entry<Integer, ClusterNode> entry = iterator.next();
            if (entry.getKey() >= hash) {
                break;
            }
        }
        
        // Collect replicas
        while (replicas.size() < replicationFactor && iterator.hasNext()) {
            ClusterNode node = iterator.next().getValue();
            if (!replicas.contains(node)) {
                replicas.add(node);
            }
        }
        
        // If we need more replicas, wrap around
        if (replicas.size() < replicationFactor) {
            iterator = ring.entrySet().iterator();
            while (replicas.size() < replicationFactor && iterator.hasNext()) {
                ClusterNode node = iterator.next().getValue();
                if (!replicas.contains(node)) {
                    replicas.add(node);
                }
            }
        }
        
        return replicas;
    }

    /**
     * Get all nodes in the cluster.
     */
    public List<ClusterNode> getAllNodes() {
        return new ArrayList<>(nodes.values());
    }

    /**
     * Get node by ID.
     */
    public ClusterNode getNode(String nodeId) {
        return nodes.get(nodeId);
    }

    /**
     * Check if a node exists.
     */
    public boolean hasNode(String nodeId) {
        return nodes.containsKey(nodeId);
    }

    /**
     * Get cluster size.
     */
    public int getClusterSize() {
        return nodes.size();
    }

    /**
     * Get ring statistics.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalNodes", nodes.size());
        stats.put("totalVirtualNodes", ring.size());
        stats.put("virtualNodesPerNode", virtualNodesPerNode);
        stats.put("replicationFactor", replicationFactor);
        
        // Calculate distribution
        Map<String, Integer> nodeDistribution = new HashMap<>();
        for (ClusterNode node : ring.values()) {
            nodeDistribution.merge(node.getId(), 1, Integer::sum);
        }
        stats.put("nodeDistribution", nodeDistribution);
        
        return stats;
    }

    /**
     * Hash function for consistent hashing.
     */
    private int hash(String key) {
        return Math.abs(key.hashCode());
    }

    /**
     * Cluster node representation.
     */
    public static class ClusterNode {
        private final String id;
        private final String host;
        private final int port;
        private final int grpcPort;
        private final int raftPort;
        private NodeState state;
        private long lastHeartbeat;

        public ClusterNode(String id, String host, int port, int grpcPort, int raftPort) {
            this.id = id;
            this.host = host;
            this.port = port;
            this.grpcPort = grpcPort;
            this.raftPort = raftPort;
            this.state = NodeState.UNKNOWN;
            this.lastHeartbeat = System.currentTimeMillis();
        }

        // Getters and setters
        public String getId() { return id; }
        public String getHost() { return host; }
        public int getPort() { return port; }
        public int getGrpcPort() { return grpcPort; }
        public int getRaftPort() { return raftPort; }
        public NodeState getState() { return state; }
        public void setState(NodeState state) { this.state = state; }
        public long getLastHeartbeat() { return lastHeartbeat; }
        public void setLastHeartbeat(long lastHeartbeat) { this.lastHeartbeat = lastHeartbeat; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ClusterNode that = (ClusterNode) o;
            return Objects.equals(id, that.id);
        }

        @Override
        public int hashCode() {
            return Objects.hash(id);
        }

        @Override
        public String toString() {
            return "ClusterNode{" +
                    "id='" + id + '\'' +
                    ", host='" + host + '\'' +
                    ", port=" + port +
                    ", state=" + state +
                    '}';
        }
    }

    /**
     * Node states in the cluster.
     */
    public enum NodeState {
        UNKNOWN, ONLINE, OFFLINE, FAILED
    }
} 