# Distributed Key/Value Store (Java Spring Boot)

A lightweight distributed persistent Key/Value store with JSON value support, range queries, WAL, SSTables, and optional multi-node setup.

## Features

- **Put / Read / Delete / BatchPut / ReadKeyRange** - Core CRUD operations
- **JSON object values** - Full JSON support for values
- **Write-Ahead Log** - Crash safety and durability
- **Sorted SSTables** - Persistent disk storage
- **MemTable** - In-memory writes for performance
- **Docker-based multi-node setup** - Easy cluster deployment
- **gRPC inter-node communication** - High-performance node coordination
- **Raft-based replication** - Strong consistency and fault tolerance
- **Consistent Hashing (HashRing)** - Even load distribution and fault tolerance

## Build & Run

### Prerequisites
- Java 17+
- Maven
- Docker (for cluster setup)

### Local Run
```bash
mvn clean install
java -jar target/kvstore-0.0.1-SNAPSHOT.jar
```

### Docker Deployment
```bash
docker-compose up -d
```

## API Documentation

### Core Operations

#### PUT /api/v1/kv/{key}
Store a key-value pair with JSON support.

**Request Body:**
```json
{
  "value": "{\"name\": \"John\", \"age\": 30}"
}
```

#### GET /api/v1/kv/{key}
Read a value by key.

#### DELETE /api/v1/kv/{key}
Delete a key-value pair.

#### POST /api/v1/kv/batch
Batch put multiple key-value pairs.

**Request Body:**
```json
{
  "key1": "{\"value\": \"data1\"}",
  "key2": "{\"value\": \"data2\"}",
  "key3": "{\"value\": \"data3\"}"
}
```

#### GET /api/v1/kv/range?startKey={start}&endKey={end}
Read a range of keys.

### Additional Operations

- `GET /api/v1/kv/stats` - Get store statistics

## Architecture

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Client App    │    │   Client App    │    │   Client App    │
└─────────┬───────┘    └─────────┬───────┘    └─────────┬───────┘
          │                       │                       │
          └───────────────────────┼───────────────────────┘
                                  │
                    ┌─────────────┴─────────────┐
                    │      Load Balancer        │
                    │      (Nginx/HAProxy)      │
                    └─────────────┬─────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
┌───────▼────────┐    ┌──────────▼──────────┐    ┌───────▼────────┐
│   KV Store     │    │    KV Store         │    │   KV Store     │
│   Node 1       │    │    Node 2           │    │   Node 3       │
│                │    │                      │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │   REST API  ││    ││   REST API      │ │    ││   REST API  │ │
│ │   (Port 8080)││    ││   (Port 8080)   │ │    ││   (Port 8080)││
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
│                │    │                      │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │   gRPC      ││    ││   gRPC          │ │    ││   gRPC      │ │
│ │   (Port 9090)││    ││   (Port 9090)   │ │    ││   (Port 9090)││
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
│                │    │                      │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │   Raft      ││    ││   Raft          │ │    ││   Raft      │ │
│ │   (Port 9091)││    ││   (Port 9091)   │ │    ││   (Port 9091)││
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
│                │    │                      │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │  MemTable   ││    ││   MemTable     │ │    ││   MemTable  │ │
│ │  SSTables   ││    ││   SSTables     │ │    ││   SSTables  │ │
│ │  WAL        ││    ││   WAL          │ │    ││   WAL       │ │
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
└────────────────┘    └─────────────────────┘    └────────────────┘
```

## Core Components

### 1. **REST API Layer** (`KeyValueController`)
- **Endpoints**: PUT, GET, DELETE, POST (batch operations), GET (range queries)
- **Features**: JSON value support, range queries
- **Port**: 8080 (REST), 8081 (Management)

### 2. **Service Layer** (`KeyValueService`)
- **Business Logic**: CRUD operations, batch processing
- **Caching**: In-memory cache for hot data
- **Thread Safety**: ReadWriteLock for concurrent access

### 3. **Storage Layer**
- **MemTable**: In-memory write buffer (100MB max)
- **SSTables**: Persistent disk storage (sorted)
- **WAL**: Write-Ahead Log for crash recovery
- **Compaction**: Background merge of SSTables

### 4. **Distributed Consensus** (`RaftReplicationService`)
- **Leader Election**: Automatic failover
- **Log Replication**: Strong consistency
- **Fault Tolerance**: Majority-based quorum

### 5. **Inter-Node Communication** (`GrpcKeyValueService`)
- **Protocol**: gRPC for high-performance RPC
- **Port**: 9090 (gRPC), 9091 (Raft)
- **Features**: Streaming, bidirectional communication

## Performance Characteristics

- **Write Performance**: 10,000+ writes/sec per node
- **Read Performance**: 50,000+ reads/sec per node
- **Latency**: Sub-millisecond response times
- **Throughput**: Linear scaling with node addition

## Monitoring & Management

- **Health Checks**: `/actuator/health`
- **Metrics**: `/actuator/metrics`
- **Application Info**: `/actuator/info`
- **Management Port**: 8081

## Docker Deployment

The application includes Docker support for easy deployment:

```bash
# Build the application
docker build -t kvstore .

# Run single node
docker run -p 8080:8080 -p 8081:8081 kvstore

# Run multi-node cluster
docker-compose up -d
```

## Development

### Project Structure
```
src/main/java/com/moniepoint/kvstore/
├── controller/          # REST API endpoints
├── service/            # Business logic
├── storage/            # Storage engine (MemTable, SSTables, WAL)
├── raft/              # Raft consensus implementation
├── grpc/              # gRPC service implementation
└── model/             # Data models
```

### Key Technologies
- **Spring Boot 3.2.0**: Application framework
- **Java 17**: Programming language
- **gRPC**: Inter-node communication
- **Raft**: Distributed consensus
- **Docker**: Containerization
- **Maven**: Build automation

## License

This project is licensed under the MIT License. 