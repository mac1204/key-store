package com.moniepoint.kvstore.controller;

import com.moniepoint.kvstore.cluster.ClusterManager;
import com.moniepoint.kvstore.cluster.HashRing;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * Controller for cluster management endpoints.
 */
@RestController
@RequestMapping("/cluster")
public class ClusterController {

    private final ClusterManager clusterManager;

    @Autowired
    public ClusterController(ClusterManager clusterManager) {
        this.clusterManager = clusterManager;
    }

    /**
     * Get smart Raft configuration.
     */
    @GetMapping("/raft/config")
    public ResponseEntity<Map<String, Object>> getRaftConfig() {
        return ResponseEntity.ok(clusterManager.getSmartRaftConfig());
    }

    /**
     * Get cluster statistics.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getClusterStats() {
        return ResponseEntity.ok(clusterManager.getClusterStats());
    }

    /**
     * Get all nodes in the cluster.
     */
    @GetMapping("/nodes")
    public ResponseEntity<List<HashRing.ClusterNode>> getAllNodes() {
        return ResponseEntity.ok(clusterManager.getAllNodes());
    }

    /**
     * Get online nodes only.
     */
    @GetMapping("/nodes/online")
    public ResponseEntity<List<HashRing.ClusterNode>> getOnlineNodes() {
        return ResponseEntity.ok(clusterManager.getOnlineNodes());
    }

    /**
     * Get node by ID.
     */
    @GetMapping("/nodes/{nodeId}")
    public ResponseEntity<HashRing.ClusterNode> getNode(@PathVariable String nodeId) {
        HashRing.ClusterNode node = clusterManager.getNode(nodeId);
        if (node != null) {
            return ResponseEntity.ok(node);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Check if a node is online.
     */
    @GetMapping("/nodes/{nodeId}/status")
    public ResponseEntity<Map<String, Object>> getNodeStatus(@PathVariable String nodeId) {
        boolean isOnline = clusterManager.isNodeOnline(nodeId);
        HashRing.ClusterNode node = clusterManager.getNode(nodeId);
        
        Map<String, Object> status = Map.of(
            "nodeId", nodeId,
            "isOnline", isOnline,
            "state", node != null ? node.getState().toString() : "UNKNOWN"
        );
        
        return ResponseEntity.ok(status);
    }

    /**
     * Add a node to the cluster.
     */
    @PostMapping("/nodes")
    public ResponseEntity<Map<String, Object>> addNode(@RequestBody Map<String, Object> request) {
        try {
            String nodeId = (String) request.get("nodeId");
            String host = (String) request.get("host");
            Integer port = (Integer) request.get("port");
            Integer grpcPort = (Integer) request.get("grpcPort");
            Integer raftPort = (Integer) request.get("raftPort");
            
            if (nodeId == null || host == null || port == null || grpcPort == null || raftPort == null) {
                return ResponseEntity.badRequest().body(Map.of("error", "Missing required fields"));
            }
            
            clusterManager.addNode(nodeId, host, port, grpcPort, raftPort);
            
            return ResponseEntity.ok(Map.of(
                "message", "Node added successfully",
                "nodeId", nodeId
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Remove a node from the cluster.
     */
    @DeleteMapping("/nodes/{nodeId}")
    public ResponseEntity<Map<String, Object>> removeNode(@PathVariable String nodeId) {
        try {
            clusterManager.removeNode(nodeId);
            return ResponseEntity.ok(Map.of(
                "message", "Node removed successfully",
                "nodeId", nodeId
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get responsible node for a key.
     */
    @GetMapping("/responsible/{key}")
    public ResponseEntity<HashRing.ClusterNode> getResponsibleNode(@PathVariable String key) {
        HashRing.ClusterNode node = clusterManager.getResponsibleNode(key);
        if (node != null) {
            return ResponseEntity.ok(node);
        } else {
            return ResponseEntity.notFound().build();
        }
    }

    /**
     * Get replica nodes for a key.
     */
    @GetMapping("/replicas/{key}")
    public ResponseEntity<List<HashRing.ClusterNode>> getReplicaNodes(@PathVariable String key) {
        List<HashRing.ClusterNode> replicas = clusterManager.getReplicaNodes(key);
        return ResponseEntity.ok(replicas);
    }

    /**
     * Check if this node is responsible for a key.
     */
    @GetMapping("/responsible/{key}/check")
    public ResponseEntity<Map<String, Object>> isResponsibleFor(@PathVariable String key) {
        boolean isResponsible = clusterManager.isResponsibleFor(key);
        HashRing.ClusterNode responsible = clusterManager.getResponsibleNode(key);
        
        Map<String, Object> result = Map.of(
            "key", key,
            "isResponsible", isResponsible,
            "responsibleNode", responsible != null ? responsible.getId() : null
        );
        
        return ResponseEntity.ok(result);
    }

    /**
     * Trigger manual failover for a node.
     */
    @PostMapping("/failover/{nodeId}")
    public ResponseEntity<Map<String, Object>> triggerFailover(@PathVariable String nodeId) {
        try {
            clusterManager.handleNodeFailure(nodeId);
            return ResponseEntity.ok(Map.of(
                "message", "Failover triggered successfully",
                "nodeId", nodeId
            ));
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }
} 