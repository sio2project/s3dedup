# s3dedup

S3 deduplication proxy server with Filetracker protocol compatibility.

## Overview

`s3dedup` is an S3 proxy layer that adds content-based deduplication capabilities while maintaining backwards compatibility with the Filetracker protocol (v2). Files with identical content are stored only once in S3, reducing storage costs and improving efficiency.

## Features

- **Content Deduplication**: Files are stored by SHA256 hash, identical content is stored only once
- **Filetracker Compatible**: Drop-in replacement for legacy Filetracker servers
- **Pluggable Storage**: Support for SQLite and PostgreSQL metadata storage
- **Migration Support**: Offline and live migration from old Filetracker instances
- **Auto Cleanup**: Background cleaner removes unreferenced S3 objects
- **Multi-bucket**: Run multiple independent buckets on different ports

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
| `S3_ENDPOINT` | *required* | S3/MinIO endpoint URL |
| `S3_ACCESS_KEY` | *required* | S3 access key |
| `S3_SECRET_KEY` | *required* | S3 secret key |
| `S3_FORCE_PATH_STYLE` | `true` | Use path-style S3 URLs |
| `CLEANER_ENABLED` | `true` | Enable background cleaner |
| `CLEANER_INTERVAL` | `3600` | Cleaner run interval (seconds) |
| `CLEANER_BATCH_SIZE` | `1000` | Cleaner batch size |
| `CLEANER_MAX_DELETES` | `10000` | Max deletions per cleaner run |
| `FILETRACKER_URL` | - | Old Filetracker URL for live migration |

For PostgreSQL, use:
```
KVSTORAGE_TYPE=postgres
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=password
POSTGRES_DB=s3dedup
POSTGRES_MAX_CONNECTIONS=10
```

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

## Migration

### Offline Migration

Migrate all files from old Filetracker while the proxy is offline:

```bash
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  migrate --env \
  --filetracker-url http://old-filetracker:8000 \
  --max-concurrency 10
```

### Live Migration

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

During live migration:
- **GET**: Falls back to old Filetracker if file not found, migrates on-the-fly
- **PUT**: Writes to both s3dedup and old Filetracker
- **DELETE**: Deletes from both systems

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
- **Lock Manager**: In-memory file-level locks for concurrent operations
- **Cleaner**: Background worker that removes unreferenced S3 objects

## License

See LICENSE file for details.
