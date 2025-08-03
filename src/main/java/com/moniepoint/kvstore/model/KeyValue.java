package com.moniepoint.kvstore.model;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.time.Instant;

/**
 * Represents a key-value pair with JSON value support.
 */
public class KeyValue {
    private String key;
    private JsonNode value; // JSON object value
    private long timestamp;
    private long ttl; // Time to live in seconds, 0 means no expiration
    private boolean deleted;
    private static final ObjectMapper objectMapper = new ObjectMapper();

    public KeyValue() {
        this.timestamp = Instant.now().getEpochSecond();
        this.deleted = false;
    }

    public KeyValue(String key, JsonNode value) {
        this();
        this.key = key;
        this.value = value;
    }

    public KeyValue(String key, JsonNode value, long ttl) {
        this(key, value);
        this.ttl = ttl;
    }

    public KeyValue(String key, String value) {
        this();
        this.key = key;
        try {
            this.value = objectMapper.readTree(value);
        } catch (Exception e) {
            // If not valid JSON, treat as string value
            this.value = objectMapper.createObjectNode().put("value", value);
        }
    }

    public KeyValue(String key, String value, long ttl) {
        this(key, value);
        this.ttl = ttl;
    }

    // Getters and Setters
    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public JsonNode getValue() {
        return value;
    }

    public void setValue(JsonNode value) {
        this.value = value;
    }

    public String getValueAsString() {
        return value != null ? value.toString() : null;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public long getTtl() {
        return ttl;
    }

    public void setTtl(long ttl) {
        this.ttl = ttl;
    }

    public boolean isDeleted() {
        return deleted;
    }

    public void setDeleted(boolean deleted) {
        this.deleted = deleted;
    }

    public boolean isExpired() {
        if (ttl == 0) {
            return false;
        }
        return Instant.now().getEpochSecond() > timestamp + ttl;
    }

    @Override
    public String toString() {
        return "KeyValue{" +
                "key='" + key + '\'' +
                ", value=" + value +
                ", timestamp=" + timestamp +
                ", ttl=" + ttl +
                ", deleted=" + deleted +
                '}';
    }
} 