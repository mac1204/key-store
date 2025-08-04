package com.moniepoint.kvstore.controller;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.moniepoint.kvstore.raft.RaftReplicationService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;
import java.util.ArrayList;

/**
 * Controller for Raft consensus protocol endpoints.
 */
@RestController
@RequestMapping("/raft")
public class RaftController {

    private final RaftReplicationService raftService;
    private final ObjectMapper objectMapper;

    @Autowired
    public RaftController(RaftReplicationService raftService) {
        this.raftService = raftService;
        this.objectMapper = new ObjectMapper();
    }

    /**
     * Handle vote requests from candidates.
     */
    @PostMapping("/vote")
    public ResponseEntity<Map<String, Object>> requestVote(@RequestBody String requestBody) {
        try {
            JsonNode request = objectMapper.readTree(requestBody);
            String candidateId = request.get("candidateId").asText();
            long term = request.get("term").asLong();
            
            RaftReplicationService.RequestVoteRequest voteRequest = 
                new RaftReplicationService.RequestVoteRequest(
                    term, candidateId, 
                    request.get("lastLogIndex").asLong(),
                    request.get("lastLogTerm").asLong()
                );
            
            RaftReplicationService.RequestVoteResponse response = 
                raftService.handleRequestVote(voteRequest);
            
            Map<String, Object> result = Map.of(
                "term", response.getTerm(),
                "voteGranted", response.isVoteGranted()
            );
            
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Handle append entries from leader.
     */
    @PostMapping("/append")
    public ResponseEntity<Map<String, Object>> appendEntries(@RequestBody String requestBody) {
        try {
            JsonNode request = objectMapper.readTree(requestBody);
            String leaderId = request.get("leaderId").asText();
            long term = request.get("term").asLong();
            long prevLogIndex = request.get("prevLogIndex").asLong();
            long prevLogTerm = request.get("prevLogTerm").asLong();
            long leaderCommit = request.get("leaderCommit").asLong();
            
            // Parse entries
            List<RaftReplicationService.LogEntry> entries = parseLogEntries(request.get("entries"));
            
            RaftReplicationService.AppendEntriesRequest appendRequest = 
                new RaftReplicationService.AppendEntriesRequest(
                    term, leaderId, prevLogIndex, prevLogTerm, entries, leaderCommit
                );
            
            RaftReplicationService.AppendEntriesResponse response = 
                raftService.handleAppendEntries(appendRequest);
            
            Map<String, Object> result = Map.of(
                "term", response.getTerm(),
                "success", response.isSuccess()
            );
            
            return ResponseEntity.ok(result);
        } catch (Exception e) {
            return ResponseEntity.badRequest().body(Map.of("error", e.getMessage()));
        }
    }

    /**
     * Get Raft node status.
     */
    @GetMapping("/status")
    public ResponseEntity<Map<String, Object>> getStatus() {
        Map<String, Object> status = Map.of(
            "nodeId", raftService.getNodeId(),
            "state", raftService.getState().toString(),
            "currentTerm", raftService.getCurrentTerm(),
            "lastLogIndex", raftService.getLastLogIndex(),
            "lastLogTerm", raftService.getLastLogTerm()
        );
        
        return ResponseEntity.ok(status);
    }

    /**
     * Parse log entries from JSON.
     */
    private List<RaftReplicationService.LogEntry> parseLogEntries(JsonNode entriesNode) {
        List<RaftReplicationService.LogEntry> entries = new ArrayList<>();
        
        if (entriesNode.isArray()) {
            for (JsonNode entryNode : entriesNode) {
                RaftReplicationService.LogEntry entry = new RaftReplicationService.LogEntry(
                    entryNode.get("term").asLong(),
                    entryNode.get("index").asLong(),
                    entryNode.get("operation").asText(),
                    entryNode.get("key").asText(),
                    entryNode.get("value").asText()
                );
                entries.add(entry);
            }
        }
        
        return entries;
    }
} 