package com.moniepoint.kvstore.storage.compaction;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Scheduler for managing background compaction tasks.
 * Provides configurable compaction policies and monitoring.
 */
@Component
public class CompactionScheduler {

    private final CompactionService compactionService;
    private final AtomicLong lastCompactionTime;
    private final AtomicLong compactionCount;

    @Value("${kvstore.compaction.enabled:true}")
    private boolean compactionEnabled;

    @Value("${kvstore.compaction.interval:300000}") // 5 minutes default
    private int compactionInterval;

    @Value("${kvstore.compaction.max-duration:300000}") // 5 minutes max
    private int maxCompactionDuration;

    @Autowired
    public CompactionScheduler(CompactionService compactionService) {
        this.compactionService = compactionService;
        this.lastCompactionTime = new AtomicLong(System.currentTimeMillis());
        this.compactionCount = new AtomicLong(0);
    }

    /**
     * Scheduled task for regular compaction checks.
     */
    @Scheduled(fixedRateString = "${kvstore.compaction.check-interval:60000}") // 1 minute default
    public void scheduledCompactionCheck() {
        if (!compactionEnabled) {
            return;
        }

        long currentTime = System.currentTimeMillis();
        long timeSinceLastCompaction = currentTime - lastCompactionTime.get();

        // Check if enough time has passed since last compaction
        if (timeSinceLastCompaction >= compactionInterval) {
            performCompaction();
        }
    }

    /**
     * Perform compaction with monitoring.
     */
    private void performCompaction() {
        if (compactionService.isCompactionInProgress()) {
            return; // Already in progress
        }

        long startTime = System.currentTimeMillis();
        
        try {
            compactionService.forceCompaction();
            compactionCount.incrementAndGet();
            lastCompactionTime.set(System.currentTimeMillis());
            
            long duration = System.currentTimeMillis() - startTime;
            if (duration > maxCompactionDuration) {
                // Log warning about long compaction
                System.out.println("Warning: Compaction took " + duration + "ms, exceeding limit of " + maxCompactionDuration + "ms");
            }
            
        } catch (Exception e) {
            System.err.println("Error during compaction: " + e.getMessage());
        }
    }

    /**
     * Get compaction statistics.
     */
    public CompactionStats getStats() {
        return new CompactionStats(
            compactionCount.get(),
            lastCompactionTime.get(),
            compactionService.isCompactionInProgress(),
            compactionEnabled,
            compactionInterval,
            maxCompactionDuration
        );
    }

    /**
     * Enable or disable compaction.
     */
    public void setCompactionEnabled(boolean enabled) {
        this.compactionEnabled = enabled;
    }

    /**
     * Set compaction interval.
     */
    public void setCompactionInterval(int interval) {
        this.compactionInterval = interval;
    }

    /**
     * Set maximum compaction duration.
     */
    public void setMaxCompactionDuration(int duration) {
        this.maxCompactionDuration = duration;
    }

    /**
     * Force immediate compaction.
     */
    public void forceCompaction() {
        performCompaction();
    }

    /**
     * Statistics class for compaction monitoring.
     */
    public static class CompactionStats {
        private final long totalCompactions;
        private final long lastCompactionTime;
        private final boolean compactionInProgress;
        private final boolean compactionEnabled;
        private final int compactionInterval;
        private final int maxCompactionDuration;

        public CompactionStats(long totalCompactions, long lastCompactionTime, 
                             boolean compactionInProgress, boolean compactionEnabled,
                             int compactionInterval, int maxCompactionDuration) {
            this.totalCompactions = totalCompactions;
            this.lastCompactionTime = lastCompactionTime;
            this.compactionInProgress = compactionInProgress;
            this.compactionEnabled = compactionEnabled;
            this.compactionInterval = compactionInterval;
            this.maxCompactionDuration = maxCompactionDuration;
        }

        // Getters
        public long getTotalCompactions() { return totalCompactions; }
        public long getLastCompactionTime() { return lastCompactionTime; }
        public boolean isCompactionInProgress() { return compactionInProgress; }
        public boolean isCompactionEnabled() { return compactionEnabled; }
        public int getCompactionInterval() { return compactionInterval; }
        public int getMaxCompactionDuration() { return maxCompactionDuration; }
    }
} 