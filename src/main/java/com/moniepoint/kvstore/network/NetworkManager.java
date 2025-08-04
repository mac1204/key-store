package com.moniepoint.kvstore.network;

import com.moniepoint.kvstore.cluster.HashRing;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.net.URI;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Manages network communication between cluster nodes.
 */
@Component
public class NetworkManager {

    private final HttpClient httpClient;
    private final ExecutorService executorService;
    private final int networkTimeout;

    public NetworkManager(@Value("${kvstore.network.timeout:5000}") int networkTimeout) {
        this.networkTimeout = networkTimeout;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(this.networkTimeout))
                .build();
        this.executorService = Executors.newCachedThreadPool();
    }

    /**
     * Send HTTP request to another node.
     */
    public CompletableFuture<HttpResponse<String>> sendRequest(String url, String method, String body) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                HttpRequest.Builder requestBuilder = HttpRequest.newBuilder()
                        .uri(URI.create(url))
                        .timeout(Duration.ofMillis(networkTimeout));

                switch (method.toUpperCase()) {
                    case "GET":
                        requestBuilder.GET();
                        break;
                    case "POST":
                        requestBuilder.POST(HttpRequest.BodyPublishers.ofString(body));
                        break;
                    case "PUT":
                        requestBuilder.PUT(HttpRequest.BodyPublishers.ofString(body));
                        break;
                    case "DELETE":
                        requestBuilder.DELETE();
                        break;
                    default:
                        throw new IllegalArgumentException("Unsupported HTTP method: " + method);
                }

                HttpRequest request = requestBuilder.build();
                return httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            } catch (IOException | InterruptedException e) {
                throw new RuntimeException("Failed to send request to " + url, e);
            }
        }, executorService);
    }

    /**
     * Send heartbeat to another node.
     */
    public CompletableFuture<Boolean> sendHeartbeat(HashRing.ClusterNode node) {
        String url = "http://" + node.getHost() + ":" + node.getPort() + "/actuator/health";
        return sendRequest(url, "GET", "")
                .thenApply(response -> response.statusCode() == 200)
                .exceptionally(throwable -> {
                    System.out.println("Heartbeat failed for " + node.getId() + ": " + throwable.getMessage());
                    return false;
                });
    }

    /**
     * Send Raft vote request.
     */
    public CompletableFuture<Boolean> sendVoteRequest(HashRing.ClusterNode node, String candidateId, long term) {
        String url = "http://" + node.getHost() + ":" + node.getRaftPort() + "/raft/vote";
        String body = String.format("{\"candidateId\":\"%s\",\"term\":%d}", candidateId, term);
        
        return sendRequest(url, "POST", body)
                .thenApply(response -> response.statusCode() == 200)
                .exceptionally(throwable -> {
                    System.out.println("Vote request failed for " + node.getId() + ": " + throwable.getMessage());
                    return false;
                });
    }

    /**
     * Send Raft append entries request.
     */
    public CompletableFuture<Boolean> sendAppendEntries(HashRing.ClusterNode node, String leaderId, long term, String entries) {
        String url = "http://" + node.getHost() + ":" + node.getRaftPort() + "/raft/append";
        String body = String.format("{\"leaderId\":\"%s\",\"term\":%d,\"entries\":%s}", leaderId, term, entries);
        
        return sendRequest(url, "POST", body)
                .thenApply(response -> response.statusCode() == 200)
                .exceptionally(throwable -> {
                    System.out.println("Append entries failed for " + node.getId() + ": " + throwable.getMessage());
                    return false;
                });
    }

    /**
     * Send key-value operation to another node.
     */
    public CompletableFuture<String> sendKeyValueOperation(HashRing.ClusterNode node, String operation, String key, String value) {
        String url = "http://" + node.getHost() + ":" + node.getPort() + "/api/v1/kv/" + key;
        
        switch (operation.toUpperCase()) {
            case "GET":
                return sendRequest(url, "GET", "")
                        .thenApply(HttpResponse::body);
            case "PUT":
                return sendRequest(url, "PUT", value)
                        .thenApply(HttpResponse::body);
            case "DELETE":
                return sendRequest(url, "DELETE", "")
                        .thenApply(HttpResponse::body);
            default:
                throw new IllegalArgumentException("Unsupported operation: " + operation);
        }
    }

    /**
     * Send batch operations to another node.
     */
    public CompletableFuture<String> sendBatchOperations(HashRing.ClusterNode node, String operations) {
        String url = "http://" + node.getHost() + ":" + node.getPort() + "/api/v1/kv/batch";
        return sendRequest(url, "POST", operations)
                .thenApply(HttpResponse::body);
    }

    /**
     * Check if a node is reachable.
     */
    public CompletableFuture<Boolean> isNodeReachable(HashRing.ClusterNode node) {
        return sendHeartbeat(node);
    }

    /**
     * Get node status.
     */
    public CompletableFuture<String> getNodeStatus(HashRing.ClusterNode node) {
        String url = "http://" + node.getHost() + ":" + node.getPort() + "/actuator/info";
        return sendRequest(url, "GET", "")
                .thenApply(HttpResponse::body)
                .exceptionally(throwable -> "{\"status\":\"unreachable\",\"error\":\"" + throwable.getMessage() + "\"}");
    }

    /**
     * Shutdown network manager.
     */
    public void shutdown() {
        executorService.shutdown();
    }
} 