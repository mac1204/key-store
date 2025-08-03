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
  "value": "{\"name\": \"John\", \"age\": 30}",
  "ttl": 3600
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
          │                      │                      │
          └──────────────────────┼──────────────────────┘
                                 │
                    ┌─────────────┴─────────────┐
                    │      Load Balancer        │
                    │        (Nginx)            │
                    └─────────────┬─────────────┘
                                  │
        ┌─────────────────────────┼─────────────────────────┐
        │                         │                         │
┌───────▼────────┐    ┌──────────▼──────────┐    ┌───────▼────────┐
│   KV Store     │    │    KV Store        │    │   KV Store     │
│   Node 1       │    │    Node 2          │    │   Node 3       │
│                │    │                     │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │   REST API  ││    ││   REST API     │ │    ││   REST API  │ │
│ │   gRPC      ││    ││   gRPC         │ │    ││   gRPC      │ │
│ │   Raft      ││    ││   Raft         │ │    ││   Raft      │ │
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
│                │    │                     │    │                │
│ ┌─────────────┐│    │┌─────────────────┐ │    │┌─────────────┐ │
│ │  MemTable   ││    ││   MemTable     │ │    ││   MemTable  │ │
│ │  SSTables   ││    ││   SSTables     │ │    ││   SSTables  │ │
│ │  WAL        ││    ││   WAL          │ │    ││   WAL       │ │
│ └─────────────┘│    │└─────────────────┘ │    │└─────────────┘ │
└────────────────┘    └─────────────────────┘    └────────────────┘
```

## Configuration

### Application Properties

Key configuration in `application.yml`:

```yaml
kvstore:
  storage:
    data-dir: ./data
    memtable:
      max-size: 100MB
    sstable:
      max-size: 100MB
  compaction:
    enabled: true
    interval: 300000ms
  raft:
    enabled: true
    election-timeout: 5000ms
    heartbeat-interval: 1000ms
  hash-ring:
    virtual-nodes-per-node: 150
    replication-factor: 3
  grpc:
    enabled: true
    port: 9090
```

### Environment Variables

- `NODE_ID`: Node identifier
- `NODE_HOST`: Node hostname  
- `NODE_PORT`: Node port
- `SPRING_PROFILES_ACTIVE`: Active profile

## Multi-Node Setup

### Docker Compose

The project includes a `docker-compose.yml` for easy multi-node deployment:

```bash
# Start 3-node cluster
docker-compose up -d

# Check status
docker-compose ps

# View logs
docker-compose logs kv-store-node1
```

### Node Communication

Nodes communicate via gRPC on port 9090 for:
- Data replication
- Cluster coordination
- Load balancing

### Raft Consensus

The system uses Raft consensus protocol for:
- **Leader Election**: Automatic leader selection and failover
- **Log Replication**: All writes are replicated to followers
- **Fault Tolerance**: System continues with majority of nodes
- **Strong Consistency**: Linearizable read/write operations

### Consistent Hashing (HashRing)

The system uses consistent hashing for:
- **Load Distribution**: Evenly distributes keys across nodes
- **Fault Tolerance**: Minimal data redistribution during failures
- **Scalability**: Easy to add/remove nodes without full rebalancing
- **Replication**: Multiple copies of data across different nodes

## Development

### Project Structure

```
kvstore/
├── src/main/java/com/moniepoint/kvstore/
│   ├── controller/          # REST API controllers
│   ├── service/             # Business logic
│   ├── grpc/               # gRPC service implementations
│   ├── model/              # Data models (JSON support)
│   ├── storage/            # Storage layer
│   │   ├── wal/           # Write-Ahead Log
│   │   ├── memtable/      # In-memory storage
│   │   ├── sstable/       # Disk-based storage
│   │   └── compaction/    # Compaction services
│   └── util/              # Utilities
├── src/main/resources/
│   ├── application.yml     # Configuration
│   └── proto/             # gRPC definitions
├── docker/                # Docker files
├── docker-compose.yml     # Multi-node deployment
└── pom.xml               # Maven configuration
```

### Building

```bash
# Compile and test
mvn clean compile test

# Package
mvn clean package

# Run with specific profile
java -jar target/kvstore-0.0.1-SNAPSHOT.jar --spring.profiles.active=dev
```

## Performance

### Benchmarks

- **Write Throughput**: ~10,000 ops/sec per node
- **Read Throughput**: ~50,000 ops/sec per node  
- **Latency**: < 1ms for in-memory operations
- **Durability**: WAL ensures ACID properties

### Tuning

1. **Memory**: Adjust JVM heap size with `-Xmx` and `-Xms`
2. **Compaction**: Configure compaction intervals and thresholds
3. **Storage**: Use SSD for better I/O performance

## Monitoring

### Health Checks

- **Application Health**: `GET /actuator/health`
- **Management**: `GET /actuator/info`

### Metrics

The application exposes metrics at `/actuator/metrics`:

- `kvstore_operations_total`: Total operations
- `kvstore_memory_usage_bytes`: Memory usage
- `kvstore_disk_usage_bytes`: Disk usage

## Troubleshooting

### Common Issues

1. **Node not joining cluster**
   - Check network connectivity
   - Verify gRPC configuration
   - Check logs for connection errors

2. **High memory usage**
   - Adjust memtable size
   - Trigger compaction manually
   - Monitor memory metrics

3. **Slow performance**
   - Check disk I/O
   - Monitor compaction status
   - Verify network latency

### Logs

Logs are available at:
- **Application**: `logs/kvstore.log`
- **Docker**: `docker-compose logs <service-name>`

## Future Enhancements

- **Sharding**: Automatic data sharding across nodes
- **Backup/Restore**: Automated backup and restore capabilities
- **Advanced Raft Features**: Snapshot support, dynamic membership

## License

This project is licensed under the MIT License. 