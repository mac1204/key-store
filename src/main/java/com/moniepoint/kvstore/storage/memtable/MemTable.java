package com.moniepoint.kvstore.storage.memtable;

import com.moniepoint.kvstore.model.KeyValue;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * MemTable implementation for in-memory storage of key-value pairs.
 * Uses a skip list for efficient range queries and ordered iteration.
 */
@Component
public class MemTable {

    private final ConcurrentSkipListMap<String, KeyValue> data;
    private final AtomicLong size;
    private final AtomicLong memoryUsage;
    private final long maxMemoryUsage;
    private volatile boolean frozen;

    public MemTable() {
        this.data = new ConcurrentSkipListMap<>();
        this.size = new AtomicLong(0);
        this.memoryUsage = new AtomicLong(0);
        this.maxMemoryUsage = 100 * 1024 * 1024; // 100MB
        this.frozen = false;
    }

    /**
     * Put a key-value pair into the memtable.
     */
    public void put(String key, KeyValue value) {
        if (frozen) {
            throw new IllegalStateException("MemTable is frozen and cannot accept new writes");
        }

        KeyValue oldValue = data.put(key, value);
        
        // Update size
        if (oldValue == null) {
            size.incrementAndGet();
        }
        
        // Update memory usage (simplified calculation)
        updateMemoryUsage(key, value, oldValue);
    }

    /**
     * Get a value by key.
     */
    public Optional<KeyValue> get(String key) {
        KeyValue value = data.get(key);
        if (value != null && !value.isDeleted()) {
            return Optional.of(value);
        }
        return Optional.empty();
    }

    /**
     * Delete a key-value pair.
     */
    public void delete(String key) {
        if (frozen) {
            throw new IllegalStateException("MemTable is frozen and cannot accept new writes");
        }

        KeyValue value = data.get(key);
        if (value != null) {
            value.setDeleted(true);
        }
    }

    /**
     * Check if a key exists.
     */
    public boolean containsKey(String key) {
        KeyValue value = data.get(key);
        return value != null && !value.isDeleted();
    }

    /**
     * Get all keys in the memtable.
     */
    public Set<String> getAllKeys() {
        return data.entrySet().stream()
                .filter(entry -> !entry.getValue().isDeleted())
                .map(Map.Entry::getKey)
                .collect(Collectors.toSet());
    }

    /**
     * Get all key-value pairs in the memtable.
     */
    public List<KeyValue> getAllKeyValues() {
        return data.values().stream()
                .filter(value -> !value.isDeleted())
                .collect(Collectors.toList());
    }

    /**
     * Get the size of the memtable.
     */
    public long size() {
        return data.size();
    }

    /**
     * Get the memory usage of the memtable.
     */
    public long getMemoryUsage() {
        return memoryUsage.get();
    }

    /**
     * Check if the memtable is full.
     */
    public boolean isFull() {
        return memoryUsage.get() >= maxMemoryUsage;
    }

    /**
     * Freeze the memtable to prevent new writes.
     */
    public void freeze() {
        this.frozen = true;
    }

    /**
     * Check if the memtable is frozen.
     */
    public boolean isFrozen() {
        return frozen;
    }

    /**
     * Clear the memtable.
     */
    public void clear() {
        data.clear();
        size.set(0);
        memoryUsage.set(0);
        frozen = false;
    }

    /**
     * Get an iterator for all entries in the memtable.
     */
    public Iterator<Map.Entry<String, KeyValue>> iterator() {
        return data.entrySet().iterator();
    }

    /**
     * Get a range of entries.
     */
    public NavigableMap<String, KeyValue> getRange(String fromKey, String toKey) {
        return data.subMap(fromKey, true, toKey, false);
    }

    /**
     * Compact the memtable by removing expired and deleted entries.
     */
    public void compact() {
        Iterator<Map.Entry<String, KeyValue>> iterator = data.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, KeyValue> entry = iterator.next();
            KeyValue value = entry.getValue();
            
            if (value.isDeleted()) {
                iterator.remove();
                size.decrementAndGet();
                // Update memory usage
                updateMemoryUsage(entry.getKey(), null, value);
            }
        }
    }

    /**
     * Update memory usage estimation.
     */
    private void updateMemoryUsage(String key, KeyValue newValue, KeyValue oldValue) {
        long keySize = key.length() * 2; // UTF-16 encoding
        
        if (oldValue != null) {
            long oldValueSize = estimateValueSize(oldValue);
            memoryUsage.addAndGet(-(keySize + oldValueSize));
        }
        
        if (newValue != null) {
            long newValueSize = estimateValueSize(newValue);
            memoryUsage.addAndGet(keySize + newValueSize);
        }
    }

    /**
     * Estimate the size of a KeyValue object in bytes.
     */
    private long estimateValueSize(KeyValue value) {
        if (value == null) return 0;
        
        long size = 0;
        if (value.getKey() != null) size += value.getKey().length() * 2;
        if (value.getValue() != null) size += value.getValueAsString().length() * 2;
        size += 8; // timestamp
        size += 1; // deleted flag
        
        return size;
    }

    /**
     * Get statistics about the memtable.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("size", size.get());
        stats.put("memoryUsage", memoryUsage.get());
        stats.put("maxMemoryUsage", maxMemoryUsage);
        stats.put("frozen", frozen);
        stats.put("loadFactor", (double) memoryUsage.get() / maxMemoryUsage);
        
        return stats;
    }

    public boolean isEmpty() {
        return data.isEmpty();
    }

    public long estimateSize() {
        long totalSize = 0;
        for (Map.Entry<String, KeyValue> entry : data.entrySet()) {
            totalSize += estimateEntrySize(entry.getKey(), entry.getValue());
        }
        return totalSize;
    }

    private long estimateEntrySize(String key, KeyValue value) {
        long size = 0;
        
        // Estimate size: key + value + timestamp + deleted flag
        size += key.length() * 2; // UTF-8 encoding
        size += value.getValueAsString().length() * 2; // UTF-8 encoding
        size += 8; // timestamp (long)
        size += 1; // deleted flag (boolean)
        
        return size;
    }
} 