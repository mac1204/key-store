package com.moniepoint.kvstore.service;

import com.moniepoint.kvstore.model.KeyValue;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Service interface for key-value store operations.
 */
public interface KeyValueService {
    
    /**
     * Put a key-value pair into the store.
     * @param key the key
     * @param value the JSON value
     * @return the stored KeyValue object
     */
    KeyValue put(String key, String value);
    
    /**
     * Put a key-value pair with TTL into the store.
     * @param key the key
     * @param value the JSON value
     * @param ttl time to live in seconds
     * @return the stored KeyValue object
     */
    KeyValue put(String key, String value, long ttl);
    
    /**
     * Read a value by key.
     * @param key the key to retrieve
     * @return Optional containing the KeyValue if found
     */
    Optional<KeyValue> read(String key);
    
    /**
     * Delete a key-value pair.
     * @param key the key to delete
     * @return true if the key was found and deleted, false otherwise
     */
    boolean delete(String key);
    
    /**
     * Batch put multiple key-value pairs.
     * @param keyValuePairs map of key-value pairs to store
     * @return list of stored KeyValue objects
     */
    List<KeyValue> batchPut(Map<String, String> keyValuePairs);
    
    /**
     * Read a range of keys.
     * @param startKey the starting key (inclusive)
     * @param endKey the ending key (exclusive)
     * @return list of KeyValue objects in the range
     */
    List<KeyValue> readKeyRange(String startKey, String endKey);
    
    /**
     * Get the total number of key-value pairs in the store.
     * @return the count of key-value pairs
     */
    long size();
} 