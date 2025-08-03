package com.moniepoint.kvstore.grpc;

import com.moniepoint.kvstore.model.KeyValue;
import com.moniepoint.kvstore.service.KeyValueService;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Optional;

/**
 * gRPC service implementation for key-value store operations.
 * Note: This is a placeholder implementation. The actual gRPC service
 * would be generated from the proto files.
 */
@Service
public class GrpcKeyValueService {

    private final KeyValueService keyValueService;

    @Autowired
    public GrpcKeyValueService(KeyValueService keyValueService) {
        this.keyValueService = keyValueService;
    }

    /**
     * Handle PUT request via gRPC.
     */
    public void put(String key, String value, long ttl, StreamObserver<KeyValueResponse> responseObserver) {
        try {
            KeyValue result = keyValueService.put(key, value, ttl);
            KeyValueResponse response = KeyValueResponse.newBuilder()
                    .setSuccess(true)
                    .setKey(result.getKey())
                    .setValue(result.getValueAsString())
                    .setTimestamp(result.getTimestamp())
                    .setTtl(result.getTtl())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            KeyValueResponse response = KeyValueResponse.newBuilder()
                    .setSuccess(false)
                    .setError(e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * Handle GET request via gRPC.
     */
    public void get(String key, StreamObserver<KeyValueResponse> responseObserver) {
        try {
            Optional<KeyValue> result = keyValueService.read(key);
            if (result.isPresent()) {
                KeyValue kv = result.get();
                KeyValueResponse response = KeyValueResponse.newBuilder()
                        .setSuccess(true)
                        .setKey(kv.getKey())
                        .setValue(kv.getValueAsString())
                        .setTimestamp(kv.getTimestamp())
                        .setTtl(kv.getTtl())
                        .build();
                responseObserver.onNext(response);
            } else {
                KeyValueResponse response = KeyValueResponse.newBuilder()
                        .setSuccess(false)
                        .setError("Key not found")
                        .build();
                responseObserver.onNext(response);
            }
            responseObserver.onCompleted();
        } catch (Exception e) {
            KeyValueResponse response = KeyValueResponse.newBuilder()
                    .setSuccess(false)
                    .setError(e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * Handle DELETE request via gRPC.
     */
    public void delete(String key, StreamObserver<DeleteResponse> responseObserver) {
        try {
            boolean deleted = keyValueService.delete(key);
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setSuccess(deleted)
                    .setMessage(deleted ? "Key deleted successfully" : "Key not found")
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        } catch (Exception e) {
            DeleteResponse response = DeleteResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Error: " + e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    /**
     * Handle GET ALL request via gRPC.
     */
    public void getAll(StreamObserver<KeyValueListResponse> responseObserver) {
        try {
            // For now, return empty list since getAllKeyValues is not in the interface
            KeyValueListResponse.Builder responseBuilder = KeyValueListResponse.newBuilder()
                    .setSuccess(true);
            
            responseObserver.onNext(responseBuilder.build());
            responseObserver.onCompleted();
        } catch (Exception e) {
            KeyValueListResponse response = KeyValueListResponse.newBuilder()
                    .setSuccess(false)
                    .setError(e.getMessage())
                    .build();
            responseObserver.onNext(response);
            responseObserver.onCompleted();
        }
    }

    // Placeholder classes for gRPC messages
    // These would normally be generated from proto files
    public static class KeyValueResponse {
        private boolean success;
        private String key;
        private String value;
        private long timestamp;
        private long ttl;
        private String error;

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private KeyValueResponse response = new KeyValueResponse();

            public Builder setSuccess(boolean success) {
                response.success = success;
                return this;
            }

            public Builder setKey(String key) {
                response.key = key;
                return this;
            }

            public Builder setValue(String value) {
                response.value = value;
                return this;
            }

            public Builder setTimestamp(long timestamp) {
                response.timestamp = timestamp;
                return this;
            }

            public Builder setTtl(long ttl) {
                response.ttl = ttl;
                return this;
            }

            public Builder setError(String error) {
                response.error = error;
                return this;
            }

            public KeyValueResponse build() {
                return response;
            }
        }
    }

    public static class DeleteResponse {
        private boolean success;
        private String message;

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private DeleteResponse response = new DeleteResponse();

            public Builder setSuccess(boolean success) {
                response.success = success;
                return this;
            }

            public Builder setMessage(String message) {
                response.message = message;
                return this;
            }

            public DeleteResponse build() {
                return response;
            }
        }
    }

    public static class KeyValueListResponse {
        private boolean success;
        private java.util.List<KeyValueResponse> keyValues;
        private String error;

        public static Builder newBuilder() {
            return new Builder();
        }

        public static class Builder {
            private KeyValueListResponse response = new KeyValueListResponse();

            public Builder setSuccess(boolean success) {
                response.success = success;
                return this;
            }

            public Builder addKeyValues(KeyValueResponse kvResponse) {
                if (response.keyValues == null) {
                    response.keyValues = new java.util.ArrayList<>();
                }
                response.keyValues.add(kvResponse);
                return this;
            }

            public Builder setError(String error) {
                response.error = error;
                return this;
            }

            public KeyValueListResponse build() {
                return response;
            }
        }
    }
} 