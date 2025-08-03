package com.moniepoint.kvstore.storage.sstable;

import com.moniepoint.kvstore.model.KeyValue;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * SSTable (Sorted String Table) Manager for handling disk-based storage.
 * Manages multiple SSTable files and provides efficient read operations.
 */
@Component
public class SSTableManager {

    private final Path sstableDirectory;
    private final Map<String, SSTable> sstables;
    private final AtomicLong nextSSTableId;
    private final long maxSSTableSize;

    public SSTableManager() throws IOException {
        this.sstableDirectory = Paths.get("data", "sstables");
        Files.createDirectories(sstableDirectory);
        
        this.sstables = new ConcurrentHashMap<>();
        this.nextSSTableId = new AtomicLong(0);
        this.maxSSTableSize = 100 * 1024 * 1024; // 100MB
        
        // Load existing SSTables
        loadExistingSSTables();
    }

    /**
     * Create a new SSTable from memtable data.
     */
    public SSTable createSSTable(Map<String, KeyValue> data) {
        try {
            String sstableId = generateSSTableId();
            Path sstablePath = sstableDirectory.resolve(sstableId + ".sst");
            
            SSTable sstable = new SSTable(sstablePath, sstableId);
            sstable.write(data);
            
            sstables.put(sstableId, sstable);
            return sstable;
        } catch (IOException e) {
            throw new RuntimeException("Failed to create SSTable", e);
        }
    }

    /**
     * Get a value by key from all SSTables.
     */
    public Optional<KeyValue> get(String key) {
        // Search in reverse order (newest first)
        List<String> sortedIds = getSortedSSTableIds();
        
        for (int i = sortedIds.size() - 1; i >= 0; i--) {
            String sstableId = sortedIds.get(i);
            SSTable sstable = sstables.get(sstableId);
            
            if (sstable != null) {
                try {
                    Optional<KeyValue> result = sstable.get(key);
                    if (result.isPresent()) {
                        KeyValue value = result.get();
                        if (!value.isExpired() && !value.isDeleted()) {
                            return result;
                        }
                    }
                } catch (IOException e) {
                    System.err.println("Error reading from SSTable " + sstableId + ": " + e.getMessage());
                }
            }
        }
        
        return Optional.empty();
    }

    /**
     * Get all keys from all SSTables.
     */
    public Set<String> getAllKeys() {
        Set<String> allKeys = new HashSet<>();
        
        for (SSTable sstable : sstables.values()) {
            try {
                allKeys.addAll(sstable.getAllKeys());
            } catch (IOException e) {
                // Log error and continue
                System.err.println("Error reading SSTable: " + e.getMessage());
            }
        }
        
        return allKeys;
    }

    /**
     * Get all key-value pairs from all SSTables.
     */
    public List<KeyValue> getAllKeyValues() {
        List<KeyValue> allValues = new ArrayList<>();
        
        for (SSTable sstable : sstables.values()) {
            try {
                allValues.addAll(sstable.getAllKeyValues());
            } catch (IOException e) {
                // Log error and continue
                System.err.println("Error reading SSTable: " + e.getMessage());
            }
        }
        
        return allValues;
    }

    /**
     * Compact SSTables by merging them.
     */
    public void compact() {
        try {
            List<String> sortedIds = getSortedSSTableIds();
            
            if (sortedIds.size() <= 1) {
                return; // Nothing to compact
            }
            
            // Merge all SSTables into a new one
            Map<String, KeyValue> mergedData = new TreeMap<>();
            
            for (String sstableId : sortedIds) {
                SSTable sstable = sstables.get(sstableId);
                if (sstable != null) {
                    try {
                        List<KeyValue> values = sstable.getAllKeyValues();
                        for (KeyValue value : values) {
                            if (!value.isExpired() && !value.isDeleted()) {
                                mergedData.put(value.getKey(), value);
                            }
                        }
                    } catch (IOException e) {
                        System.err.println("Error reading SSTable " + sstableId + ": " + e.getMessage());
                    }
                }
            }
            
            // Create new SSTable with merged data
            SSTable newSSTable = createSSTable(mergedData);
            
            // Remove old SSTables
            for (String sstableId : sortedIds) {
                SSTable oldSSTable = sstables.remove(sstableId);
                if (oldSSTable != null) {
                    try {
                        oldSSTable.delete();
                    } catch (IOException e) {
                        System.err.println("Error deleting SSTable " + sstableId + ": " + e.getMessage());
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to compact SSTables", e);
        }
    }

    /**
     * Get statistics about all SSTables.
     */
    public Map<String, Object> getStats() {
        Map<String, Object> stats = new HashMap<>();
        stats.put("totalSSTables", sstables.size());
        stats.put("totalSize", getTotalSize());
        
        List<Map<String, Object>> sstableStats = new ArrayList<>();
        for (SSTable sstable : sstables.values()) {
            sstableStats.add(sstable.getStats());
        }
        stats.put("sstableDetails", sstableStats);
        
        return stats;
    }

    /**
     * Load existing SSTables from disk.
     */
    private void loadExistingSSTables() throws IOException {
        if (!Files.exists(sstableDirectory)) {
            return;
        }
        
        Files.list(sstableDirectory)
                .filter(path -> path.toString().endsWith(".sst"))
                .forEach(path -> {
                    try {
                        String filename = path.getFileName().toString();
                        String sstableId = filename.substring(0, filename.length() - 4);
                        
                        SSTable sstable = new SSTable(path, sstableId);
                        sstables.put(sstableId, sstable);
                        
                        // Update next SSTable ID
                        long id = Long.parseLong(sstableId);
                        if (id >= nextSSTableId.get()) {
                            nextSSTableId.set(id + 1);
                        }
                    } catch (Exception e) {
                        System.err.println("Error loading SSTable " + path + ": " + e.getMessage());
                    }
                });
    }

    /**
     * Generate a unique SSTable ID.
     */
    private String generateSSTableId() {
        return String.valueOf(nextSSTableId.getAndIncrement());
    }

    /**
     * Get sorted list of SSTable IDs.
     */
    private List<String> getSortedSSTableIds() {
        List<String> ids = new ArrayList<>(sstables.keySet());
        ids.sort(Comparator.comparingLong(Long::parseLong));
        return ids;
    }

    /**
     * Get total size of all SSTables.
     */
    private long getTotalSize() {
        return sstables.values().stream()
                .mapToLong(sstable -> {
                    try {
                        return sstable.getSize();
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .sum();
    }

    /**
     * Inner class representing a single SSTable.
     */
    public static class SSTable {
        private final Path filePath;
        private final String id;
        private final Map<String, Long> index; // key -> file position

        public SSTable(Path filePath, String id) {
            this.filePath = filePath;
            this.id = id;
            this.index = new HashMap<>();
        }

        /**
         * Write data to SSTable.
         */
        public void write(Map<String, KeyValue> data) throws IOException {
            try (DataOutputStream dos = new DataOutputStream(
                    new BufferedOutputStream(Files.newOutputStream(filePath)))) {
                
                for (Map.Entry<String, KeyValue> entry : data.entrySet()) {
                    String key = entry.getKey();
                    KeyValue value = entry.getValue();
                    
                    // Record position
                    index.put(key, (long) dos.size());
                    
                    // Write entry
                    writeEntry(dos, key, value);
                }
            }
        }

        /**
         * Get a value by key.
         */
        public Optional<KeyValue> get(String key) throws IOException {
            Long position = index.get(key);
            if (position == null) {
                return Optional.empty();
            }
            
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(filePath)))) {
                
                dis.skipBytes(position.intValue());
                return readEntry(dis);
            }
        }

        /**
         * Get all keys.
         */
        public Set<String> getAllKeys() throws IOException {
            return new HashSet<>(index.keySet());
        }

        /**
         * Get all key-value pairs.
         */
        public List<KeyValue> getAllKeyValues() throws IOException {
            List<KeyValue> values = new ArrayList<>();
            
            try (DataInputStream dis = new DataInputStream(
                    new BufferedInputStream(Files.newInputStream(filePath)))) {
                
                while (dis.available() > 0) {
                    Optional<KeyValue> entry = readEntry(dis);
                    if (entry.isPresent()) {
                        values.add(entry.get());
                    }
                }
            }
            
            return values;
        }

        /**
         * Get the size of the SSTable file.
         */
        public long getSize() throws IOException {
            return Files.size(filePath);
        }

        /**
         * Delete the SSTable file.
         */
        public void delete() throws IOException {
            Files.deleteIfExists(filePath);
        }

        /**
         * Write a single entry to the output stream.
         */
        private void writeEntry(DataOutputStream dos, String key, KeyValue value) throws IOException {
            dos.writeUTF(key);
            dos.writeUTF(value.getValueAsString());
            dos.writeLong(value.getTimestamp());
            dos.writeLong(value.getTtl());
            dos.writeBoolean(value.isDeleted());
        }

        /**
         * Read a single entry from the input stream.
         */
        private Optional<KeyValue> readEntry(DataInputStream dis) throws IOException {
            try {
                String key = dis.readUTF();
                String value = dis.readUTF();
                long timestamp = dis.readLong();
                long ttl = dis.readLong();
                boolean deleted = dis.readBoolean();
                
                KeyValue kv = new KeyValue(key, value, ttl);
                kv.setTimestamp(timestamp);
                kv.setDeleted(deleted);
                
                return Optional.of(kv);
            } catch (EOFException e) {
                return Optional.empty();
            }
        }

        /**
         * Get statistics about this SSTable.
         */
        public Map<String, Object> getStats() {
            Map<String, Object> stats = new HashMap<>();
            stats.put("id", id);
            stats.put("indexSize", index.size());
            
            try {
                stats.put("fileSize", getSize());
            } catch (IOException e) {
                stats.put("fileSize", -1);
            }
            
            return stats;
        }
    }
} 