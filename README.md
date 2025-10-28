# s3dedup

S3 deduplication proxy server with Filetracker protocol compatibility.

## Overview

`s3dedup` is an S3 proxy layer that adds content-based deduplication capabilities while maintaining backwards compatibility with the Filetracker protocol (v2). Files with identical content are stored only once in S3, reducing storage costs and improving efficiency.

## Features

- **Content Deduplication**: Files are stored by SHA256 hash, identical content is stored only once
- **Filetracker Compatible**: Drop-in replacement for legacy Filetracker servers
- **Pluggable Storage**: Support for SQLite and PostgreSQL metadata storage
- **Distributed Locking**: PostgreSQL advisory locks for distributed, high-availability deployments
- **Migration Support**: Offline and live migration from old Filetracker instances
- **Auto Cleanup**: Background cleaner removes unreferenced S3 objects
- **Single-instance per bucket**: Each instance handles exactly one bucket; scale horizontally with multiple instances

## Quick Start with Docker

Pull the image from GitHub Container Registry:

```bash
docker pull ghcr.io/sio2project/s3dedup:latest
```

Run with environment variables:

```bash
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  -v s3dedup-data:/app/data \
  -e S3_ENDPOINT=http://minio:9000 \
  -e S3_ACCESS_KEY=minioadmin \
  -e S3_SECRET_KEY=minioadmin \
  ghcr.io/sio2project/s3dedup:latest
```

Or use an environment file:

```bash
# Copy and customize .env.example
cp .env.example .env

# Run with env file
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  -v s3dedup-data:/app/data \
  --env-file .env \
  ghcr.io/sio2project/s3dedup:latest
```

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `LOG_LEVEL` | `info` | Logging level (trace, debug, info, warn, error) |
| `LOG_JSON` | `false` | Enable JSON logging |
| `BUCKET_NAME` | `default` | Bucket name identifier |
| `LISTEN_ADDRESS` | `0.0.0.0` | Server bind address |
| `LISTEN_PORT` | `8080` | Server port |
| `KVSTORAGE_TYPE` | `sqlite` | KV storage backend (sqlite, postgres) |
| `SQLITE_PATH` | `/app/data/kv.db` | SQLite database path |
| `SQLITE_MAX_CONNECTIONS` | `10` | SQLite connection pool size |
| `LOCKS_TYPE` | `memory` | Lock manager backend (memory, postgres) |
| `S3_ENDPOINT` | *required* | S3/MinIO endpoint URL |
| `S3_ACCESS_KEY` | *required* | S3 access key |
| `S3_SECRET_KEY` | *required* | S3 secret key |
| `S3_FORCE_PATH_STYLE` | `true` | Use path-style S3 URLs |
| `CLEANER_ENABLED` | `true` | Enable background cleaner |
| `CLEANER_INTERVAL` | `3600` | Cleaner run interval (seconds) |
| `CLEANER_BATCH_SIZE` | `1000` | Cleaner batch size |
| `CLEANER_MAX_DELETES` | `10000` | Max deletions per cleaner run |
| `FILETRACKER_URL` | - | Old Filetracker URL for live migration (HTTP fallback) |
| `FILETRACKER_V1_DIR` | - | V1 Filetracker directory for filesystem-based migration |

### PostgreSQL Configuration

For PostgreSQL KV storage, use:
```
KVSTORAGE_TYPE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=s3dedup
POSTGRES_MAX_CONNECTIONS=10
```

### Distributed Locking (PostgreSQL Advisory Locks)

For high-availability deployments with multiple s3dedup instances, enable PostgreSQL-based distributed locks:

```
LOCKS_TYPE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=s3dedup
POSTGRES_MAX_CONNECTIONS=10
```

**Benefits of PostgreSQL Locks**:
- **Distributed Locking**: Multiple s3dedup instances can safely coordinate file operations
- **High Availability**: If one instance fails, others can continue with the same locks
- **Load Balancing**: Multiple instances can share the same database for coordinated access
- **Atomic Operations**: Prevents race conditions in concurrent file operations

**How It Works**:
- Uses PostgreSQL's built-in advisory locks (`pg_advisory_lock`, `pg_advisory_lock_shared`)
- Lock keys are hashed to 64-bit integers for PostgreSQL's lock API
- Shared locks allow concurrent reads; exclusive locks ensure serialized writes
- Automatic lock release when guard is dropped (via background cleanup tasks)

**Note**: PostgreSQL locks require the same PostgreSQL instance used for KV storage. Connection pool is shared between both uses.

### Connection Pool Sizing

The `POSTGRES_MAX_CONNECTIONS` setting controls the maximum number of concurrent database connections from a single s3dedup instance. This **single pool** is shared between KV storage operations and lock management.

**How to Choose Pool Size:**

```
Pool Size = (Concurrent Requests × 1.5) + Lock Overhead
```

**General Guidelines:**

| Deployment | Concurrency | Recommended Pool Size | Notes |
|------------|-------------|----------------------|-------|
| **Low** | 1-5 concurrent requests | 10 | Default, suitable for development/testing |
| **Medium** | 5-20 concurrent requests | 20-30 | Small production deployments |
| **High** | 20-100 concurrent requests | 50-100 | Large production deployments |
| **Very High** | 100+ concurrent requests | 100-200 | Use multiple instances with load balancing |

**Factors to Consider:**

1. **Number of s3dedup Instances**
   - If you have N instances, each needs its own pool
   - Total connections = N instances × pool_size
   - PostgreSQL must have enough capacity for all instances
   - Example: 3 instances × 30 pool_size = 90 connections needed

2. **Lock Contention**
   - File operations acquire locks (1 connection per lock)
   - Concurrent uploads/downloads increase lock pressure
   - Add 20% overhead for lock operations
   - Example: 20 concurrent requests → pool_size = (20 × 1.5) + overhead ≈ 35

3. **Database Configuration**
   - Check PostgreSQL `max_connections` setting
   - Reserve connections for maintenance, monitoring, backups
   - Example: PostgreSQL with 200 max_connections:
     - Reserve 10 for maintenance
     - If 3 s3dedup instances: (200 - 10) / 3 ≈ 63 per instance

4. **Memory Usage Per Connection**
   - Each connection uses ~5-10 MB of memory
   - Pool size 50 = ~250-500 MB per instance
   - Monitor actual usage and adjust accordingly

**Example Configurations:**

**Development (1 instance, low throughput):**
```json
"postgres": {
  "pool_size": 10
}
```

**Production (3 instances, medium throughput):**
```json
"postgres": {
  "pool_size": 30
}
```
With PostgreSQL `max_connections = 100`:
- 3 × 30 = 90 connections (10 reserved)

**High-Availability (5 instances, high throughput with PostgreSQL max_connections = 200):**
```json
"postgres": {
  "pool_size": 35
}
```
- 5 × 35 = 175 connections (25 reserved for other operations)

**Monitoring and Tuning:**

Monitor these metrics to optimize pool size:

1. **Connection Utilization**: Check if connections are frequently exhausted
   ```sql
   SELECT count(*) FROM pg_stat_activity WHERE datname = 's3dedup';
   ```

2. **Lock Wait Times**: Monitor if operations wait for available connections
3. **Memory Usage**: Watch instance memory as pool size increases

**Scaling Strategy:**

- **Start Conservative**: Begin with pool_size = 10-20
- **Monitor Usage**: Track connection utilization over 1-2 weeks
- **Increase Gradually**: Increment by 10-20 when you see high utilization
- **Scale Horizontally**: Instead of very large pools (>100), use more instances with moderate pools

### Config File

Alternatively, use a JSON config file:

```bash
docker run -d \
  -p 8080:8080 \
  -v $(pwd)/config.json:/app/config.json \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  server --config /app/config.json
```

Environment variables override config file values.

## Deployment and Scaling

### Single-Instance per Bucket Architecture

s3dedup follows a **single-bucket-per-instance** design pattern, consistent with 12-factor application principles:

- **One Instance = One Bucket**: Each s3dedup instance manages exactly one S3 bucket and serves one Filetracker endpoint
- **Horizontal Scaling**: For multiple buckets, run multiple s3dedup instances (one per bucket)
- **Simplified Configuration**: Cleaner config files, easier to reason about, better for container orchestration

### High-Availability Deployments

For a single bucket with high availability, run multiple instances with PostgreSQL locks and shared database:

```bash
# All instances share the same PostgreSQL database and use PostgreSQL locks
docker run -d \
  --name s3dedup-ha-1 \
  -p 8001:8080 \
  -e BUCKET_NAME=files \
  -e LISTEN_PORT=8080 \
  -e KVSTORAGE_TYPE=postgres \
  -e LOCKS_TYPE=postgres \
  -e POSTGRES_HOST=postgres-db \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=password \
  -e POSTGRES_DB=s3dedup \
  -e S3_ENDPOINT=http://minio:9000 \
  -e S3_ACCESS_KEY=minioadmin \
  -e S3_SECRET_KEY=minioadmin \
  ghcr.io/sio2project/s3dedup:latest server --env

# Repeat for instances 2, 3, etc., on different ports
```

**Benefits of HA Setup**:
- **Load Balancing**: Requests can be distributed across multiple instances
- **Fault Tolerance**: If one instance fails, others continue serving requests
- **Coordinated Access**: PostgreSQL locks ensure safe concurrent file operations
- **Shared Metadata**: Single database prevents data inconsistency

## Migration

> **📖 Complete Migration Guide**: See [docs/migration.md](docs/migration.md) for comprehensive migration instructions

s3dedup supports migration from both Filetracker V1 (filesystem-based) and V2 (HTTP-based) servers.

### V2 Migration (Filetracker 2.1+)

#### Offline Migration

Migrate all files from Filetracker V2 via HTTP while the proxy is offline:

```bash
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  migrate --env \
  --filetracker-url http://old-filetracker:8000 \
  --max-concurrency 10
```

#### Live Migration (Zero Downtime)

Run the proxy while migrating in the background:

```bash
# Set FILETRACKER_URL in your .env file
echo "FILETRACKER_URL=http://old-filetracker:8000" >> .env

# Start in live migration mode
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  -v s3dedup-data:/app/data \
  --env-file .env \
  ghcr.io/sio2project/s3dedup:latest \
  live-migrate --env --max-concurrency 10
```

During V2 live migration:
- **GET**: Falls back to old Filetracker if file not found, migrates on-the-fly
- **PUT**: Writes to both s3dedup and old Filetracker
- **DELETE**: Deletes from both systems

### V1 Migration (Legacy Filetracker)

V1 Filetracker stores files directly on the filesystem and serves them via a simple HTTP protocol. 
The key difference from V2 is that V1 doesn't have a `/list/` endpoint for file discovery, so migration uses 
filesystem walking.

**Performance**: V1 migration uses chunked processing to handle millions of files efficiently without loading 
all file paths into memory. The filesystem is scanned in chunks of 10,000 files, keeping memory usage constant 
regardless of total file count.

#### Offline Migration

Migrate from V1 filesystem (requires access to `$FILETRACKER_DIR`):

```bash
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  -v /path/to/filetracker:/filetracker:ro \
  ghcr.io/sio2project/s3dedup:latest \
  migrate-v1 --env \
  --v1-directory /filetracker \
  --max-concurrency 10
```

#### Live Migration

Run the proxy while migrating from V1 in the background:

```bash
# With both filesystem access and HTTP fallback
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  -v s3dedup-data:/app/data \
  -v /path/to/filetracker:/filetracker:ro \
  --env-file .env \
  ghcr.io/sio2project/s3dedup:latest \
  live-migrate-v1 --env \
  --v1-directory /filetracker \
  --filetracker-url http://old-filetracker-v1:8000 \
  --max-concurrency 10

# Or with HTTP fallback only (no filesystem access)
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  -v s3dedup-data:/app/data \
  --env-file .env \
  ghcr.io/sio2project/s3dedup:latest \
  live-migrate-v1 --env \
  --filetracker-url http://old-filetracker-v1:8000 \
  --max-concurrency 10
```

During V1 live migration:
- **Background filesystem migration**: If `--v1-directory` is provided, filesystem is scanned in chunks to migrate all files
  - Chunked processing handles millions of files with constant memory usage
- **HTTP fallback**: If `--filetracker-url` is provided, GET requests fall back to V1 server if file not found
  - Automatically migrates files on first access
- **New requests**: Server accepts PUT/GET/DELETE requests normally during migration

For detailed migration strategies, performance tuning, troubleshooting, and rollback procedures, see the [Migration Guide](docs/migration.md).

## API Endpoints

Compatible with Filetracker protocol v2:

- `GET /ft/version` - Get protocol version
- `GET /ft/list/{path}` - List files
- `GET /ft/files/{path}` - Download file
- `HEAD /ft/files/{path}` - Get file metadata
- `PUT /ft/files/{path}` - Upload file
- `DELETE /ft/files/{path}` - Delete file

## Building from Source

```bash
# Build binary
cargo build --release

# Build Docker image
docker build -t s3dedup:1.0.0-dev .

# Run tests
cargo test
```

## Development

```bash
# Run with Docker Compose (includes MinIO)
docker-compose up

# Run locally
cargo run -- server --config config.json
```

## Architecture

- **API Layer**: Axum-based HTTP server with Filetracker routes
- **Deduplication**: SHA256-based content addressing
- **Storage Backend**: S3-compatible object storage (MinIO, AWS S3, etc.)
- **Metadata Store**: SQLite or PostgreSQL for file metadata and reference counts
- **Lock Manager**: In-memory (single-instance) or PostgreSQL advisory locks (distributed, multi-instance HA)
  - Memory locks: Fast, suitable for single-instance deployments
  - PostgreSQL locks: Distributed coordination, suitable for multi-instance HA setups
- **Cleaner**: Background worker that removes unreferenced S3 objects

For detailed architecture documentation, see [docs/deduplication.md](docs/deduplication.md).

## Documentation

- **[Migration Guide](docs/migration.md)** - Migrating from Filetracker v2.1+ (offline and live migration strategies)
- **[Deduplication Architecture](docs/deduplication.md)** - How content-based deduplication works, data flows, and performance characteristics

## License

See LICENSE file for details.
