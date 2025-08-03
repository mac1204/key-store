package com.moniepoint.kvstore.storage.wal;

import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Write-Ahead Log implementation for durability and crash recovery.
 * Ensures that all operations are logged before being applied to the store.
 */
@Component
public class WriteAheadLog {

    private final Path logDirectory;
    private final Path currentLogFile;
    private final ReentrantReadWriteLock lock;
    private DataOutputStream logWriter;
    private DataInputStream logReader;
    private long currentLogSize;
    private final long maxLogSize = 100 * 1024 * 1024; // 100MB

    public WriteAheadLog() throws IOException {
        this.logDirectory = Paths.get("data", "wal");
        Files.createDirectories(logDirectory);
        
        this.currentLogFile = logDirectory.resolve("wal.log");
        this.lock = new ReentrantReadWriteLock();
        
        // Create or open log file
        if (!Files.exists(currentLogFile)) {
            Files.createFile(currentLogFile);
        }
        
        this.logWriter = new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(currentLogFile, java.nio.file.StandardOpenOption.APPEND))
        );
        this.logReader = new DataInputStream(
            new BufferedInputStream(Files.newInputStream(currentLogFile))
        );
        
        this.currentLogSize = Files.size(currentLogFile);
    }

    /**
     * Append a log entry to the WAL.
     */
    public void appendLogEntry(LogEntry entry) throws IOException {
        lock.writeLock().lock();
        try {
            byte[] entryBytes = serializeLogEntry(entry);
            logWriter.writeInt(entryBytes.length);
            logWriter.write(entryBytes);
            logWriter.flush();
            currentLogSize += 4 + entryBytes.length;
            
            // Rotate log if it gets too large
            if (currentLogSize > maxLogSize) {
                rotateLog();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Read all log entries from the WAL.
     */
    public List<LogEntry> readAllLogEntries() throws IOException {
        lock.readLock().lock();
        try {
            List<LogEntry> entries = new ArrayList<>();
            logReader.reset();
            
            while (logReader.available() > 0) {
                try {
                    int entrySize = logReader.readInt();
                    byte[] entryBytes = new byte[entrySize];
                    logReader.readFully(entryBytes);
                    
                    LogEntry entry = deserializeLogEntry(entryBytes);
                    entries.add(entry);
                } catch (EOFException e) {
                    break;
                }
            }
            
            return entries;
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * Rotate the log file when it gets too large.
     */
    private void rotateLog() throws IOException {
        String timestamp = String.valueOf(System.currentTimeMillis());
        Path oldLogFile = logDirectory.resolve("wal." + timestamp + ".log");
        Files.move(currentLogFile, oldLogFile);
        
        // Create new log file
        Files.createFile(currentLogFile);
        logWriter.close();
        
        // Reopen writer for new file
        this.logWriter = new DataOutputStream(
            new BufferedOutputStream(Files.newOutputStream(currentLogFile))
        );
        this.currentLogSize = 0;
    }

    /**
     * Serialize a log entry to bytes.
     */
    private byte[] serializeLogEntry(LogEntry entry) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream dos = new DataOutputStream(baos)) {
            
            dos.writeLong(entry.getTerm());
            dos.writeLong(entry.getIndex());
            dos.writeUTF(entry.getOperation());
            dos.writeUTF(entry.getKey());
            dos.writeUTF(entry.getValue());
            dos.writeLong(entry.getTtl());
            dos.writeLong(entry.getTimestamp());
            
            return baos.toByteArray();
        }
    }

    /**
     * Deserialize bytes to a log entry.
     */
    private LogEntry deserializeLogEntry(byte[] bytes) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
             DataInputStream dis = new DataInputStream(bais)) {
            
            long term = dis.readLong();
            long index = dis.readLong();
            String operation = dis.readUTF();
            String key = dis.readUTF();
            String value = dis.readUTF();
            long ttl = dis.readLong();
            long timestamp = dis.readLong();
            
            return new LogEntry(term, index, operation, key, value, ttl, timestamp);
        }
    }

    /**
     * Close the WAL.
     */
    public void close() throws IOException {
        lock.writeLock().lock();
        try {
            if (logWriter != null) {
                logWriter.close();
            }
            if (logReader != null) {
                logReader.close();
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * Log entry representing a single operation.
     */
    public static class LogEntry {
        private final long term;
        private final long index;
        private final String operation;
        private final String key;
        private final String value;
        private final long ttl;
        private final long timestamp;

        public LogEntry(long term, long index, String operation, String key, String value, long ttl) {
            this(term, index, operation, key, value, ttl, System.currentTimeMillis());
        }

        public LogEntry(long term, long index, String operation, String key, String value, long ttl, long timestamp) {
            this.term = term;
            this.index = index;
            this.operation = operation;
            this.key = key;
            this.value = value;
            this.ttl = ttl;
            this.timestamp = timestamp;
        }

        // Getters
        public long getTerm() { return term; }
        public long getIndex() { return index; }
        public String getOperation() { return operation; }
        public String getKey() { return key; }
        public String getValue() { return value; }
        public long getTtl() { return ttl; }
        public long getTimestamp() { return timestamp; }

        @Override
        public String toString() {
            return "LogEntry{" +
                    "term=" + term +
                    ", index=" + index +
                    ", operation='" + operation + '\'' +
                    ", key='" + key + '\'' +
                    ", value='" + value + '\'' +
                    ", ttl=" + ttl +
                    ", timestamp=" + timestamp +
                    '}';
        }
    }
} 