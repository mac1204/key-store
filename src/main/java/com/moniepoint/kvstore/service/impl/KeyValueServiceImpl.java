package com.moniepoint.kvstore.service.impl;

import com.moniepoint.kvstore.model.KeyValue;
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
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Implementation of KeyValueService using MemTable, WAL, and SSTables.
 */
@Service
@Primary
public class KeyValueServiceImpl implements KeyValueService {

    private final MemTable memTable;
    private final SSTableManager sstableManager;
    private final WriteAheadLog wal;
    private final ReadWriteLock lock;
    private final Map<String, KeyValue> cache;

    @Autowired
    public KeyValueServiceImpl(MemTable memTable, SSTableManager sstableManager, WriteAheadLog wal) {
        this.memTable = memTable;
        this.sstableManager = sstableManager;
        this.wal = wal;
        this.lock = new ReentrantReadWriteLock();
        this.cache = new ConcurrentHashMap<>();
    }

    @Override
    public KeyValue put(String key, String value) {
        return put(key, value, 0);
    }

    @Override
    public KeyValue put(String key, String value, long ttl) {
        lock.writeLock().lock();
        try {
            // Create KeyValue object
            KeyValue keyValue = new KeyValue(key, value, ttl);
            
            // Write to WAL first for durability
            wal.appendLogEntry(new WriteAheadLog.LogEntry(
                0, // term (not used in simple implementation)
                System.currentTimeMillis(), // index
                "PUT",
                key,
                value,
                ttl,
                System.currentTimeMillis()
            ));
            
            // Store in memtable
            memTable.put(key, keyValue);
            
            // Update cache
            cache.put(key, keyValue);
            
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
            // First check cache
            KeyValue cached = cache.get(key);
            if (cached != null && !cached.isExpired() && !cached.isDeleted()) {
                return Optional.of(cached);
            }
            
            // Check memtable
            Optional<KeyValue> memTableResult = memTable.get(key);
            if (memTableResult.isPresent()) {
                KeyValue value = memTableResult.get();
                if (!value.isExpired() && !value.isDeleted()) {
                    cache.put(key, value);
                    return Optional.of(value);
                }
            }
            
            // Check SSTables
            Optional<KeyValue> sstableResult = sstableManager.get(key);
            if (sstableResult.isPresent()) {
                KeyValue value = sstableResult.get();
                if (!value.isExpired() && !value.isDeleted()) {
                    cache.put(key, value);
                    return Optional.of(value);
                }
            }
            
            return Optional.empty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean delete(String key) {
        lock.writeLock().lock();
        try {
            // Check if key exists
            Optional<KeyValue> existing = read(key);
            if (!existing.isPresent()) {
                return false;
            }
            
            // Create tombstone entry
            KeyValue tombstone = new KeyValue(key, "{}", 0);
            tombstone.setDeleted(true);
            
            // Write to WAL
            try {
                wal.appendLogEntry(new WriteAheadLog.LogEntry(
                    0, // term
                    System.currentTimeMillis(), // index
                    "DELETE",
                    key,
                    "",
                    0,
                    System.currentTimeMillis()
                ));
            } catch (IOException e) {
                throw new RuntimeException("Failed to write delete to WAL", e);
            }
            
            // Update memtable
            memTable.delete(key);
            
            // Update cache
            cache.remove(key);
            
            return true;
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
                String key = entry.getKey();
                String value = entry.getValue();
                
                KeyValue keyValue = put(key, value);
                results.add(keyValue);
            }
            
            return results;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public List<KeyValue> readKeyRange(String startKey, String endKey) {
        lock.readLock().lock();
        try {
            List<KeyValue> results = new ArrayList<>();
            
            // Get range from memtable
            NavigableMap<String, KeyValue> memTableRange = memTable.getRange(startKey, endKey);
            for (KeyValue value : memTableRange.values()) {
                if (!value.isExpired() && !value.isDeleted()) {
                    results.add(value);
                }
            }
            
            // Get range from SSTables (simplified - in real implementation would merge with memtable)
            // For now, just get all values and filter by range
            List<KeyValue> sstableValues = sstableManager.getAllKeyValues();
            for (KeyValue value : sstableValues) {
                if (value.getKey().compareTo(startKey) >= 0 && 
                    value.getKey().compareTo(endKey) < 0 &&
                    !value.isExpired() && !value.isDeleted()) {
                    results.add(value);
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
            long memTableSize = memTable.size();
            long sstableSize = sstableManager.getAllKeyValues().size();
            return memTableSize + sstableSize;
        } finally {
            lock.readLock().unlock();
        }
    }
} 