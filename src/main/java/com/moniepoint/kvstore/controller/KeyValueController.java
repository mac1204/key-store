package com.moniepoint.kvstore.controller;

import com.moniepoint.kvstore.model.KeyValue;
import com.moniepoint.kvstore.service.KeyValueService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * REST controller for key-value store operations.
 */
@RestController
@RequestMapping("/api/v1/kv")
public class KeyValueController {

    private final KeyValueService keyValueService;

    @Autowired
    public KeyValueController(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    /**
     * PUT endpoint to store a key-value pair.
     */
    @PutMapping("/{key}")
    public ResponseEntity<KeyValue> put(
            @PathVariable String key,
            @RequestBody Map<String, Object> request) {
        
        String value = (String) request.get("value");
        KeyValue result = keyValueService.put(key, value);
        return ResponseEntity.ok(result);
    }

    /**
     * GET endpoint to read a value by key.
     */
    @GetMapping("/{key}")
    public ResponseEntity<KeyValue> read(@PathVariable String key) {
        return keyValueService.read(key)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    /**
     * DELETE endpoint to remove a key-value pair.
     */
    @DeleteMapping("/{key}")
    public ResponseEntity<Void> delete(@PathVariable String key) {
        boolean deleted = keyValueService.delete(key);
        return deleted ? ResponseEntity.ok().build() : ResponseEntity.notFound().build();
    }

    /**
     * POST endpoint for batch put operations.
     */
    @PostMapping("/batch")
    public ResponseEntity<List<KeyValue>> batchPut(@RequestBody Map<String, String> keyValuePairs) {
        List<KeyValue> results = keyValueService.batchPut(keyValuePairs);
        return ResponseEntity.ok(results);
    }

    /**
     * GET endpoint to read a range of keys.
     */
    @GetMapping("/range")
    public ResponseEntity<List<KeyValue>> readKeyRange(
            @RequestParam String startKey,
            @RequestParam String endKey) {
        List<KeyValue> results = keyValueService.readKeyRange(startKey, endKey);
        return ResponseEntity.ok(results);
    }

    /**
     * GET endpoint to get store statistics.
     */
    @GetMapping("/stats")
    public ResponseEntity<Map<String, Object>> getStats() {
        Map<String, Object> stats = Map.of(
                "size", keyValueService.size(),
                "timestamp", System.currentTimeMillis()
        );
        return ResponseEntity.ok(stats);
    }
} 