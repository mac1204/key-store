#!/bin/bash

# Exit on any error
set -e

echo "Starting KV Store..."

# Create necessary directories
mkdir -p /app/data/wal
mkdir -p /app/data/sstables
mkdir -p /app/logs

# Set permissions
chmod 755 /app/data
chmod 755 /app/logs

# Check if we're running in a cluster
if [ -n "$CLUSTER_MODE" ]; then
    echo "Running in cluster mode..."
    export SPRING_PROFILES_ACTIVE=cluster
fi

# Set JVM options
if [ -z "$JAVA_OPTS" ]; then
    export JAVA_OPTS="-Xmx2g -Xms1g"
fi

# Start the application
exec java $JAVA_OPTS -jar /app/target/kvstore-0.0.1-SNAPSHOT.jar 