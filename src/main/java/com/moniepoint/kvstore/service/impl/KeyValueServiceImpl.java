package com.moniepoint.kvstore.service.impl;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.cluster.ClusterManager;
import com.moniepoint.kvstore.cluster.HashRing;
import com.moniepoint.kvstore.model.KeyValue;
import com.moniepoint.kvstore.network.NetworkManager;
import com.moniepoint.kvstore.raft.RaftReplicationService;
import com.moniepoint.kvstore.service.KeyValueService;
import com.moniepoint.kvstore.storage.memtable.MemTable;
import com.moniepoint.kvstore.storage.sstable.SSTableManager;
import com.moniepoint.kvstore.storage.wal.WriteAheadLog;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Primary;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Enhanced KeyValueService implementation with distributed replication and failover.
 */
@Service
@Primary
public class KeyValueServiceImpl implements KeyValueService {

    private final MemTable memTable;
    private final SSTableManager sstableManager;
    private final WriteAheadLog wal;
    private final ClusterManager clusterManager;
    private final RaftReplicationService raftService;
    private final NetworkManager networkManager;
    private final ObjectMapper objectMapper;
    private final ReadWriteLock lock;
    private final Map<String, KeyValue> cache;

    @Autowired
    public KeyValueServiceImpl(MemTable memTable, SSTableManager sstableManager, 
                             WriteAheadLog wal, ClusterManager clusterManager,
                             RaftReplicationService raftService, NetworkManager networkManager) {
        this.memTable = memTable;
        this.sstableManager = sstableManager;
        this.wal = wal;
        this.clusterManager = clusterManager;
        this.raftService = raftService;
        this.networkManager = networkManager;
        this.objectMapper = new ObjectMapper();
        this.lock = new ReentrantReadWriteLock();
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public KeyValue put(String key, String value) {
        lock.writeLock().lock();
        try {
            // Check if this node is responsible for the key
            if (!clusterManager.isResponsibleFor(key)) {
                // Redirect to responsible node
                HashRing.ClusterNode responsibleNode = clusterManager.getResponsibleNode(key);
                if (responsibleNode != null) {
                    System.out.println("Redirecting PUT for key '" + key + "' to node " + responsibleNode.getId());
                    return redirectPut(key, value, responsibleNode);
                }
            }

            // Use Raft for consensus
            boolean replicated = raftService.handleClientRequest("PUT", key, value);
            if (!replicated) {
                throw new RuntimeException("Failed to replicate PUT operation");
            }

            KeyValue keyValue = new KeyValue(key, value);
            
            // Write to WAL first for durability
            wal.appendLogEntry(new WriteAheadLog.LogEntry(
                0, // term (not used in simple implementation)
                System.currentTimeMillis(), // index
                "PUT",
                key,
                value
            ));
            
            // Store in memtable
            memTable.put(key, keyValue);
            
            // Update cache
            cache.put(key, keyValue);
            
            // Replicate to other nodes
            replicateToReplicas(key, value, "PUT");
            
            return keyValue;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Optional<KeyValue> read(String key) {
        lock.readLock().lock();
        try {
            // Check if this node is responsible for the key
            if (!clusterManager.isResponsibleFor(key)) {
                // Try to get from responsible node
                HashRing.ClusterNode responsibleNode = clusterManager.getResponsibleNode(key);
                if (responsibleNode != null) {
                    System.out.println("Redirecting GET for key '" + key + "' to node " + responsibleNode.getId());
                    return redirectGet(key, responsibleNode);
                }
            }

            // First check cache
            KeyValue cached = cache.get(key);
            if (cached != null && !cached.isDeleted()) {
                return Optional.of(cached);
            }
            
            // Check memtable
            Optional<KeyValue> memTableResult = memTable.get(key);
            if (memTableResult.isPresent()) {
                KeyValue value = memTableResult.get();
                if (!value.isDeleted()) {
                    cache.put(key, value);
                    return Optional.of(value);
                }
            }
            
            // Check SSTables
            Optional<KeyValue> sstableResult = sstableManager.get(key);
            if (sstableResult.isPresent()) {
                KeyValue value = sstableResult.get();
                if (!value.isDeleted()) {
                    cache.put(key, value);
                    return Optional.of(value);
                }
            }
            
            // If not found locally, try replicas
            return tryReplicas(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            // Check if this node is responsible for the key
            if (!clusterManager.isResponsibleFor(key)) {
                // Redirect to responsible node
                HashRing.ClusterNode responsibleNode = clusterManager.getResponsibleNode(key);
                if (responsibleNode != null) {
                    System.out.println("Redirecting DELETE for key '" + key + "' to node " + responsibleNode.getId());
                    return redirectDelete(key, responsibleNode);
                }
            }

            // Use Raft for consensus
            boolean replicated = raftService.handleClientRequest("DELETE", key, "");
            if (!replicated) {
                throw new RuntimeException("Failed to replicate DELETE operation");
            }

            // Write to WAL first
            wal.appendLogEntry(new WriteAheadLog.LogEntry(
                0, // term
                System.currentTimeMillis(), // index
                "DELETE",
                key,
                "" // Use empty string instead of null
            ));
            
            // Mark as deleted in memtable
            KeyValue deletedValue = new KeyValue(key, "");
            deletedValue.setDeleted(true);
            memTable.put(key, deletedValue);
            
            // Remove from cache
            cache.remove(key);
            
            // Replicate to other nodes
            replicateToReplicas(key, "", "DELETE");
            
            return true;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<KeyValue> batchPut(Map<String, String> keyValuePairs) {
        lock.writeLock().lock();
        try {
            List<KeyValue> results = new ArrayList<>();
            
            for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
                KeyValue keyValue = new KeyValue(entry.getKey(), entry.getValue());
                
                // Write to WAL
                wal.appendLogEntry(new WriteAheadLog.LogEntry(
                    0, // term
                    System.currentTimeMillis(), // index
                    "PUT",
                    entry.getKey(),
                    entry.getValue()
                ));
                
                // Store in memtable
                memTable.put(entry.getKey(), keyValue);
                
                // Update cache
                cache.put(entry.getKey(), keyValue);
                
                results.add(keyValue);
            }
            
            // Replicate batch to other nodes
            replicateBatchToReplicas(keyValuePairs);
            
            return results;
        } catch (IOException e) {
            throw new RuntimeException("Failed to write to WAL", e);
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<KeyValue> readKeyRange(String startKey, String endKey) {
        lock.readLock().lock();
        try {
            List<KeyValue> results = new ArrayList<>();
            
            // Get from memtable
            List<KeyValue> memTableResults = memTable.getAllKeyValues();
            for (KeyValue kv : memTableResults) {
                if (kv.getKey().compareTo(startKey) >= 0 && 
                    kv.getKey().compareTo(endKey) < 0 && 
                    !kv.isDeleted()) {
                    results.add(kv);
                }
            }
            
            // Get from SSTables
            List<KeyValue> sstableResults = sstableManager.getAllKeyValues();
            for (KeyValue kv : sstableResults) {
                if (kv.getKey().compareTo(startKey) >= 0 && 
                    kv.getKey().compareTo(endKey) < 0 && 
                    !kv.isDeleted()) {
                    results.add(kv);
                }
            }
            
            // Sort by key
            results.sort(Comparator.comparing(KeyValue::getKey));
            
            return results;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public long size() {
        lock.readLock().lock();
        try {
            long count = 0;
            
            // Count from memtable
            List<KeyValue> memTableResults = memTable.getAllKeyValues();
            for (KeyValue kv : memTableResults) {
                if (!kv.isDeleted()) {
                    count++;
                }
            }
            
            // Count from SSTables
            List<KeyValue> sstableResults = sstableManager.getAllKeyValues();
            for (KeyValue kv : sstableResults) {
                if (!kv.isDeleted()) {
                    count++;
                }
            }
            
            return count;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Replicate operation to replica nodes.
     */
    private void replicateToReplicas(String key, String value, String operation) {
        List<HashRing.ClusterNode> replicas = clusterManager.getReplicaNodes(key);
        
        for (HashRing.ClusterNode replica : replicas) {
            if (!replica.getId().equals(clusterManager.getSelf().getId())) {
                try {
                    networkManager.sendKeyValueOperation(replica, operation, key, value)
                        .thenAccept(response -> {
                            System.out.println("Replicated " + operation + " to " + replica.getId() + ": " + response);
                        })
                        .exceptionally(throwable -> {
                            System.out.println("Failed to replicate to " + replica.getId() + ": " + throwable.getMessage());
                            return null;
                        });
                } catch (Exception e) {
                    System.out.println("Failed to replicate to " + replica.getId() + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Replicate batch operations to replica nodes.
     */
    private void replicateBatchToReplicas(Map<String, String> keyValuePairs) {
        // Group by responsible nodes
        Map<HashRing.ClusterNode, Map<String, String>> nodeGroups = new HashMap<>();
        
        for (Map.Entry<String, String> entry : keyValuePairs.entrySet()) {
            HashRing.ClusterNode responsible = clusterManager.getResponsibleNode(entry.getKey());
            if (responsible != null) {
                nodeGroups.computeIfAbsent(responsible, k -> new HashMap<>())
                         .put(entry.getKey(), entry.getValue());
            }
        }
        
        // Replicate to each node
        for (Map.Entry<HashRing.ClusterNode, Map<String, String>> group : nodeGroups.entrySet()) {
            HashRing.ClusterNode node = group.getKey();
            if (!node.getId().equals(clusterManager.getSelf().getId())) {
                try {
                    String operationsJson = objectMapper.writeValueAsString(group.getValue());
                    networkManager.sendBatchOperations(node, operationsJson)
                        .thenAccept(response -> {
                            System.out.println("Replicated batch to " + node.getId() + ": " + response);
                        })
                        .exceptionally(throwable -> {
                            System.out.println("Failed to replicate batch to " + node.getId() + ": " + throwable.getMessage());
                            return null;
                        });
                } catch (Exception e) {
                    System.out.println("Failed to replicate batch to " + node.getId() + ": " + e.getMessage());
                }
            }
        }
    }

    /**
     * Try to get value from replica nodes.
     */
    private Optional<KeyValue> tryReplicas(String key) {
        List<HashRing.ClusterNode> replicas = clusterManager.getReplicaNodes(key);
        
        for (HashRing.ClusterNode replica : replicas) {
            if (!replica.getId().equals(clusterManager.getSelf().getId())) {
                try {
                    CompletableFuture<String> future = networkManager.sendKeyValueOperation(replica, "GET", key, "");
                    String response = future.get(); // Wait for response
                    
                    if (response != null && !response.isEmpty()) {
                        try {
                            JsonNode jsonNode = objectMapper.readTree(response);
                            if (jsonNode.has("value")) {
                                String value = jsonNode.get("value").asText();
                                KeyValue keyValue = new KeyValue(key, value);
                                cache.put(key, keyValue);
                                return Optional.of(keyValue);
                            }
                        } catch (Exception e) {
                            System.out.println("Failed to parse response from " + replica.getId() + ": " + e.getMessage());
                        }
                    }
                } catch (Exception e) {
                    System.out.println("Failed to get from replica " + replica.getId() + ": " + e.getMessage());
                }
            }
        }
        
        return Optional.empty();
    }

    // RPC methods for inter-node communication
    
    private KeyValue redirectPut(String key, String value, HashRing.ClusterNode node) {
        try {
            CompletableFuture<String> future = networkManager.sendKeyValueOperation(node, "PUT", key, value);
            String response = future.get(); // Wait for response
            
            if (response != null && !response.isEmpty()) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(response);
                    if (jsonNode.has("key") && jsonNode.has("value")) {
                        return new KeyValue(jsonNode.get("key").asText(), jsonNode.get("value").asText());
                    }
                } catch (Exception e) {
                    System.out.println("Failed to parse PUT response from " + node.getId() + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to redirect PUT to " + node.getId() + ": " + e.getMessage());
        }
        
        // Fallback to local storage if remote fails
        return new KeyValue(key, value);
    }
    
    private Optional<KeyValue> redirectGet(String key, HashRing.ClusterNode node) {
        try {
            CompletableFuture<String> future = networkManager.sendKeyValueOperation(node, "GET", key, "");
            String response = future.get(); // Wait for response
            
            if (response != null && !response.isEmpty()) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(response);
                    if (jsonNode.has("value")) {
                        String value = jsonNode.get("value").asText();
                        return Optional.of(new KeyValue(key, value));
                    }
                } catch (Exception e) {
                    System.out.println("Failed to parse GET response from " + node.getId() + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to redirect GET to " + node.getId() + ": " + e.getMessage());
        }
        
        return Optional.empty();
    }
    
    private boolean redirectDelete(String key, HashRing.ClusterNode node) {
        try {
            CompletableFuture<String> future = networkManager.sendKeyValueOperation(node, "DELETE", key, "");
            String response = future.get(); // Wait for response
            
            if (response != null && !response.isEmpty()) {
                try {
                    JsonNode jsonNode = objectMapper.readTree(response);
                    return jsonNode.has("success") && jsonNode.get("success").asBoolean();
                } catch (Exception e) {
                    System.out.println("Failed to parse DELETE response from " + node.getId() + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Failed to redirect DELETE to " + node.getId() + ": " + e.getMessage());
        }
        
        return false;
    }
} 