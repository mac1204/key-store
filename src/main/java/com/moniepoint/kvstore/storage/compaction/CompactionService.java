package com.moniepoint.kvstore.storage.compaction;

import com.moniepoint.kvstore.storage.memtable.MemTable;
import com.moniepoint.kvstore.storage.sstable.SSTableManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Service for managing background compaction tasks.
 * Handles memtable to SSTable conversion and SSTable merging.
 */
@Service
public class CompactionService {

    private final MemTable memTable;
    private final SSTableManager sstableManager;
    private final AtomicBoolean compactionInProgress;

    @Autowired
    public CompactionService(MemTable memTable, SSTableManager sstableManager) {
        this.memTable = memTable;
        this.sstableManager = sstableManager;
        this.compactionInProgress = new AtomicBoolean(false);
    }

    /**
     * Trigger memtable compaction when it's full.
     */
    public void triggerMemTableCompaction() {
        if (memTable.isFull() && !compactionInProgress.get()) {
            performMemTableCompaction();
        }
    }

    /**
     * Perform memtable to SSTable conversion.
     */
    private void performMemTableCompaction() {
        if (compactionInProgress.compareAndSet(false, true)) {
            try {
                // Freeze the current memtable
                memTable.freeze();
                
                // Convert memtable data to map
                java.util.Map<String, com.moniepoint.kvstore.model.KeyValue> data = new java.util.TreeMap<>();
                for (com.moniepoint.kvstore.model.KeyValue kv : memTable.getAllKeyValues()) {
                    data.put(kv.getKey(), kv);
                }
                
                // Create new SSTable
                sstableManager.createSSTable(data);
                
                // Clear the memtable
                memTable.clear();
                
            } finally {
                compactionInProgress.set(false);
            }
        }
    }

    /**
     * Trigger SSTable compaction.
     */
    public void triggerSSTableCompaction() {
        if (!compactionInProgress.get()) {
            performSSTableCompaction();
        }
    }

    /**
     * Perform SSTable merging compaction.
     */
    private void performSSTableCompaction() {
        if (compactionInProgress.compareAndSet(false, true)) {
            try {
                sstableManager.compact();
            } finally {
                compactionInProgress.set(false);
            }
        }
    }

    /**
     * Scheduled task to check for compaction opportunities.
     */
    @Scheduled(fixedRate = 60000) // Run every minute
    public void scheduledCompaction() {
        // Check if memtable needs compaction
        triggerMemTableCompaction();
        
        // Check if SSTables need compaction
        triggerSSTableCompaction();
    }

    /**
     * Get compaction status.
     */
    public boolean isCompactionInProgress() {
        return compactionInProgress.get();
    }

    /**
     * Force immediate compaction.
     */
    public void forceCompaction() {
        performMemTableCompaction();
        performSSTableCompaction();
    }
} 