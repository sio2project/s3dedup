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
  -e S3_ENDPOINT=http://garage:3900 \
  -e S3_ACCESS_KEY=your_access_key \
  -e S3_SECRET_KEY=your_secret_key \
  ghcr.io/sio2project/s3dedup:latest
```

The S3 backend can be any S3-compatible storage service (Garage, MinIO, AWS S3, etc.).

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
| `LOG_JSON` | `false` | Enable JSON logging on stdout |
| `JSON_LOG_PATH` | - | Path for JSON log file (enables dual output: pretty stdout + JSON file) |
| `BUCKET_NAME` | `default` | Bucket name identifier |
| `LISTEN_ADDRESS` | `0.0.0.0` | Server bind address |
| `LISTEN_PORT` | `8080` | Server port |
| `KVSTORAGE_TYPE` | `sqlite` | KV storage backend (sqlite, postgres) |
| `SQLITE_PATH` | `/app/data/kv.db` | SQLite database path |
| `SQLITE_MAX_CONNECTIONS` | `10` | SQLite connection pool size |
| `LOCKS_TYPE` | `memory` | Lock manager backend (memory, postgres) |
| `S3_ENDPOINT` | *required* | S3-compatible endpoint URL (Garage, MinIO, AWS, etc.) |
| `S3_ACCESS_KEY` | *required* | S3 access key |
| `S3_SECRET_KEY` | *required* | S3 secret key |
| `S3_FORCE_PATH_STYLE` | `true` | Use path-style S3 URLs |
| `S3_REGION` | `garage` | S3 region name |
| `S3_KEY_SHARDING_ENABLED` | `true` | Distribute S3 objects into subdirectories by hash prefix |
| `S3_KEY_SHARDING_DEPTH` | `2` | Number of sharding levels (e.g. 2 → `ab/cd/abcdef...`) |
| `MAX_INMEMORY_SIZE` | `67108864` | Max file size (bytes) to buffer in memory before spilling to disk (default 64MB) |
| `TEMP_DIR` | system default | Directory for temporary files; set to disk-backed path if /tmp is tmpfs |
| `CLEANER_ENABLED` | `true` | Enable background cleaner |
| `CLEANER_INTERVAL` | `3600` | Lightweight cleaner run interval (seconds) |
| `CLEANER_BATCH_SIZE` | `1000` | Cleaner batch size |
| `CLEANER_CONCURRENCY` | `8` | Number of concurrent cleaner workers |
| `CLEANER_FULL_SCAN_CRON` | - | Cron schedule for full S3 scan (5-field, e.g. `0 3 * * *`) |
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

For distributed locking across multiple instances in high-availability setups, enable PostgreSQL-based advisory locks:

```
LOCKS_TYPE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=s3dedup
POSTGRES_MAX_CONNECTIONS=10
```

**When to Use**:
- **Single-instance deployments**: Use default memory-based locking (LOCKS_TYPE=memory)
- **Multi-instance HA deployments**: Use PostgreSQL-based locking for coordinated access

**Note**: PostgreSQL locks share the connection pool with KV storage. Ensure sufficient pool size for concurrent operations. See [DEVELOPMENT.md](DEVELOPMENT.md) for implementation details.

### Connection Pool Sizing

The `POSTGRES_MAX_CONNECTIONS` setting controls the maximum number of concurrent database connections. This pool is shared between KV storage operations and lock management.

**Quick Start Recommendations:**
- **Development**: `POSTGRES_MAX_CONNECTIONS=10`
- **Small Production (1-3 instances)**: `POSTGRES_MAX_CONNECTIONS=20-30`
- **Large Production (5+ instances)**: `POSTGRES_MAX_CONNECTIONS=50-100`

For detailed pool sizing guidance, monitoring strategies, and tuning considerations, see [DEVELOPMENT.md](DEVELOPMENT.md#connection-pool-sizing).

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
  -e S3_ENDPOINT=http://garage:3900 \
  -e S3_ACCESS_KEY=your_access_key \
  -e S3_SECRET_KEY=your_secret_key \
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

#### Advanced Migration Modes

**Proxy-only mode** — forward requests to Filetracker without background migration (useful as a first phase for very large instances):

```bash
docker run -d ... ghcr.io/sio2project/s3dedup:latest \
  live-migrate --env --proxy-only
```

**File-list mode** — migrate from a pre-generated list instead of calling `/list/` (resilient to Filetracker downtime):

```bash
# Generate file list from Filetracker filesystem
find /var/lib/filetracker/links -type l | sed 's|^/var/lib/filetracker/links||' > files.txt

docker run -d ... -v $(pwd)/files.txt:/app/files.txt:ro \
  ghcr.io/sio2project/s3dedup:latest \
  live-migrate --env --file-list /app/files.txt --max-concurrency 10
```

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

Operational endpoints:

- `GET /metrics` - Prometheus metrics
- `GET /metrics/json` - Metrics in JSON format
- `GET /health` - Health check (returns 200/503 with JSON status)


## Testing

For comprehensive testing guide, see **[DEVELOPMENT.md](DEVELOPMENT.md)**.

Quick start:

```bash
# Run unit tests (no external dependencies)
cargo test --lib

# Run all tests (requires PostgreSQL + Garage)
docker-compose up -d
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"
export S3_ACCESS_KEY=GK0123456789abcdef01234567
export S3_SECRET_KEY=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
cargo test --features test-mocks
docker-compose down
```

## Development

See **[DEVELOPMENT.md](DEVELOPMENT.md)** for detailed development instructions including:

- Building from source
- Running tests with different configurations
- PostgreSQL advisory lock implementation details
- Contributing guidelines
- Performance considerations

Quick start:

```bash
# Run with Docker Compose (includes PostgreSQL + Garage S3)
docker-compose up -d

# Run tests (fixed dev credentials, no manual setup needed)
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"
export S3_ACCESS_KEY=GK0123456789abcdef01234567
export S3_SECRET_KEY=abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789
cargo test --features test-mocks
```

## Architecture

- **API Layer**: Axum-based HTTP server with Filetracker routes
- **Deduplication**: SHA256-based content addressing
- **Storage Backend**: S3-compatible object storage (Garage, MinIO, AWS S3, etc.)
- **Metadata Store**: SQLite or PostgreSQL for file metadata and reference counts
- **Lock Manager**: In-memory (single-instance) or PostgreSQL advisory locks (distributed, multi-instance HA)
  - Memory locks: Fast, suitable for single-instance deployments
  - PostgreSQL locks: Distributed coordination, suitable for multi-instance HA setups
- **Cleaner**: Background worker that removes unreferenced S3 objects (lightweight DB phases on interval, optional full S3 scan on cron)
- **Observability**: Prometheus metrics (`/metrics`), JSON metrics (`/metrics/json`), health check (`/health`)

For detailed architecture documentation, see:
- [docs/deduplication.md](docs/deduplication.md) - Deduplication architecture and performance
- [DEVELOPMENT.md](DEVELOPMENT.md) - Lock implementation details and code architecture

## Documentation

- **[Development Guide](DEVELOPMENT.md)** - Building, testing, lock implementation details, and contributing
- **[Migration Guide](docs/migration.md)** - Migrating from Filetracker v2.1+ (offline and live migration strategies)
- **[Deduplication Architecture](docs/deduplication.md)** - How content-based deduplication works, data flows, and performance characteristics

## License

See LICENSE file for details.
