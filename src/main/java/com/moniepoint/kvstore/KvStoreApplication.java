package com.moniepoint.kvstore;

import com.moniepoint.kvstore.cluster.ClusterManager;
import com.moniepoint.kvstore.raft.RaftReplicationService;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.event.EventListener;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * Main application class for the Distributed Key-Value Store.
 */
@SpringBootApplication
@EnableScheduling
@EnableConfigurationProperties
public class KvStoreApplication {

    private final ClusterManager clusterManager;
    private final RaftReplicationService raftService;

    public KvStoreApplication(ClusterManager clusterManager, RaftReplicationService raftService) {
        this.clusterManager = clusterManager;
        this.raftService = raftService;
    }

    public static void main(String[] args) {
        SpringApplication.run(KvStoreApplication.class, args);
    }

    /**
     * Initialize distributed services after application startup.
     */
    @EventListener(ApplicationReadyEvent.class)
    public void initializeDistributedServices() {
        System.out.println("🚀 Initializing Distributed Key-Value Store...");

        // Initialize cluster management
        clusterManager.initialize();

        // Start Raft consensus
        raftService.start();

        System.out.println("✅ Distributed services initialized successfully!");
        System.out.println("📊 Cluster Stats: " + clusterManager.getClusterStats());
    }
} 