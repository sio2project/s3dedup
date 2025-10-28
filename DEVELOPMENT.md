# Development Guide

This guide covers building, testing, and contributing to s3dedup.

## Building from Source

```bash
# Build binary
cargo build

# Build for release
cargo build --release

# Run in development mode
cargo run -- server --config config.json

# Format code
cargo fmt

# Run linter
cargo clippy

# Check for compilation errors without building
cargo check
```

## Testing

### Quick Start

```bash
# Run all unit tests (no external dependencies)
cargo test --lib

# Run all tests (requires PostgreSQL + MinIO)
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"
cargo test
```

### Unit Tests

Run all unit tests without external dependencies:

```bash
# Run all unit tests
cargo test --lib

# Run specific test
cargo test --lib test_hash_key_deterministic

# Run with output
cargo test --lib -- --nocapture
```

### Integration Tests

Integration tests require external services. Run specific tests with appropriate setup:

```bash
# Run all integration tests (requires PostgreSQL + MinIO)
cargo test --test integration_test

# Run specific integration test (requires PostgreSQL + MinIO)
cargo test --test integration_test test_put_and_get_file -- --nocapture
```

### PostgreSQL Lock Tests

The PostgreSQL advisory locks implementation requires a running PostgreSQL instance.

#### Setup PostgreSQL for Testing

**Option 1: Docker (Recommended)**

```bash
# Start PostgreSQL container
docker run -d \
  --name postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=s3dedup_test \
  -p 5432:5432 \
  postgres:15
```

**Option 2: Local PostgreSQL Installation**

```bash
# Create test database (requires PostgreSQL installed locally)
psql -U postgres -c "CREATE DATABASE s3dedup_test;"
```

#### Run PostgreSQL Lock Tests

Once PostgreSQL is running:

```bash
# Set DATABASE_URL environment variable
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"

# Run all PostgreSQL lock tests
cargo test --test postgres_locks_test -- --nocapture

# Run specific PostgreSQL lock test
cargo test --test postgres_locks_test test_exclusive_lock_mutual_exclusion -- --nocapture

# Run with debug logging
RUST_LOG=debug cargo test --test postgres_locks_test -- --nocapture
```

#### PostgreSQL Lock Tests Overview

The `postgres_locks_test.rs` suite validates PostgreSQL advisory lock functionality:

- **test_postgres_locks_creation**: Verifies lock system initialization
- **test_exclusive_lock_mutual_exclusion**: Ensures exclusive locks block concurrent acquisitions
- **test_shared_locks_concurrent**: Verifies multiple shared locks can coexist
- **test_exclusive_blocks_shared**: Ensures exclusive locks block shared locks
- **test_different_keys_independent**: Verifies different lock keys don't interfere
- **test_lock_release_on_guard_drop**: Ensures locks release when guard is dropped

**Lock Release Mechanism**: Both memory and PostgreSQL locks must call `.release().await` before being dropped. For memory locks, this explicitly drops the Tokio RwLock guard. For PostgreSQL locks, this calls the PostgreSQL advisory unlock function.

### Migration Tests

Run migration tests with PostgreSQL:

```bash
# Set DATABASE_URL environment variable
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"

# Run all migration tests
cargo test --test migration_test -- --nocapture

# Run specific migration test
cargo test --test migration_test test_offline_migration_empty -- --nocapture
```

### Cleaner Tests

Run background cleaner tests with PostgreSQL:

```bash
# Set DATABASE_URL environment variable
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"

# Run all cleaner tests
cargo test --test cleaner_test -- --nocapture
```

### Full Integration Testing Setup

For complete integration testing with all services:

```bash
# Option 1: Using Docker Compose (recommended for CI/CD)
docker-compose up -d

# Run all tests
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"
cargo test

# Cleanup
docker-compose down
```

**Option 2: Manual Setup**

```bash
# Terminal 1: Start PostgreSQL
docker run -d \
  --name postgres-test \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=s3dedup_test \
  -p 5432:5432 \
  postgres:15

# Terminal 2: Start MinIO
docker run -d \
  --name minio-test \
  -p 9000:9000 \
  -e MINIO_ROOT_USER=minioadmin \
  -e MINIO_ROOT_PASSWORD=minioadmin \
  minio/minio:latest server /data

# Terminal 3: Run tests
export DATABASE_URL="postgres://postgres:postgres@localhost:5432/s3dedup_test"
cargo test

# Cleanup
docker stop postgres-test minio-test
docker rm postgres-test minio-test
```

### Test Categories

| Test Type | Command | Dependencies | Time |
|-----------|---------|--------------|------|
| Unit tests | `cargo test --lib` | None | ~1-2s |
| PostgreSQL lock tests | `cargo test --test postgres_locks_test` | PostgreSQL | ~5-10s |
| Migration tests | `cargo test --test migration_test` | PostgreSQL | ~10-20s |
| Integration tests | `cargo test --test integration_test` | PostgreSQL + MinIO | ~20-30s |
| Cleaner tests | `cargo test --test cleaner_test` | PostgreSQL | ~5-10s |
| All tests | `cargo test` | PostgreSQL + MinIO | ~30-50s |

### Environment Variables for Testing

```bash
# PostgreSQL connection (required for integration/lock/migration tests)
DATABASE_URL=postgres://postgres:postgres@localhost:5432/s3dedup_test

# Logging during tests
RUST_LOG=debug              # Show debug logs
RUST_LOG=s3dedup=debug     # Only s3dedup crate logs
RUST_BACKTRACE=1           # Enable backtraces for panics
```

### Troubleshooting Tests

**PostgreSQL Connection Refused**

```bash
# Verify PostgreSQL is running
docker ps | grep postgres

# Check connection manually
psql -U postgres -h localhost -d s3dedup_test -c "SELECT 1;"
```

**Database Already Exists Error**

```bash
# Drop and recreate test database
dropdb -U postgres s3dedup_test || true
createdb -U postgres s3dedup_test
```

**Test Hangs or Times Out**

```bash
# Run with timeout
timeout 30 cargo test --test postgres_locks_test test_exclusive_lock_mutual_exclusion -- --nocapture

# Check PostgreSQL locks status
psql -U postgres -d s3dedup_test -c "SELECT * FROM pg_locks WHERE locktype = 'advisory';"
```

## Connection Pool Sizing

The `POSTGRES_MAX_CONNECTIONS` setting controls the maximum number of concurrent database connections from a single s3dedup instance. This **single pool** is shared between KV storage operations and lock management.

### How to Choose Pool Size

```
Pool Size = (Concurrent Requests × 1.5) + Lock Overhead
```

### General Guidelines

| Deployment | Concurrency | Recommended Pool Size | Notes |
|------------|-------------|----------------------|-------|
| **Low** | 1-5 concurrent requests | 10 | Default, suitable for development/testing |
| **Medium** | 5-20 concurrent requests | 20-30 | Small production deployments |
| **High** | 20-100 concurrent requests | 50-100 | Large production deployments |
| **Very High** | 100+ concurrent requests | 100-200 | Use multiple instances with load balancing |

### Factors to Consider

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

### Example Configurations

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

### Monitoring and Tuning

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

## Lock Implementation Details

### Memory Locks

Memory-based locks use Tokio RwLock for efficient single-instance deployments:

- **Shared Locks**: Multiple readers can hold the lock simultaneously
- **Exclusive Locks**: Only one writer can hold the lock
- **Release**: Explicitly dropping the Tokio guard via `.release().await`
- **Cleanup**: Lock entries are cleaned up from the HashMap when no references remain

### PostgreSQL Locks

PostgreSQL advisory locks provide distributed locking for multi-instance deployments:

- **Session-Scoped**: Locks are tied to a database connection
- **Shared vs Exclusive**: Identical semantics to memory locks but database-enforced
- **Explicit Release**: Must call `.release().await` to unlock before connection returns to pool
- **Key Hashing**: Lock keys are hashed to 64-bit integers for PostgreSQL's lock API
- **Atomic Release**: Both lock release and connection return to pool happen atomically

### Lock Guard Pattern

Both implementations follow the same pattern:

```rust
// Acquire lock
let guard = lock.acquire_exclusive().await?;

// ... do work ...

// Explicitly release
guard.release().await?;  // Calls drop in memory locks, pg_advisory_unlock in PostgreSQL
```

This unified pattern ensures consistent behavior across both backends.

## Architecture

For detailed architecture documentation, see:

- **[docs/deduplication.md](docs/deduplication.md)** - How content-based deduplication works, data flows, and performance characteristics
- **[docs/migration.md](docs/migration.md)** - Migration strategies and procedures

## Key Components

### Locks Module (`src/locks/`)

- **`mod.rs`**: Trait definitions and enums
- **`memory.rs`**: In-memory lock implementation using Tokio RwLock
- **`postgres.rs`**: PostgreSQL advisory lock implementation

### Routes Module (`src/routes/ft/`)

- **`get_file.rs`**: GET endpoint with shared locks
- **`put_file.rs`**: PUT endpoint with exclusive locks
- **`delete_file.rs`**: DELETE endpoint with exclusive locks
- **`utils.rs`**: Shared utilities for Filetracker protocol

### Storage Module (`src/kvstorage/`)

- **`mod.rs`**: Trait definitions
- **`sqlite.rs`**: SQLite backend implementation
- **`postgres.rs`**: PostgreSQL backend implementation

## Code Style

Follow Rust conventions:

```bash
# Format code
cargo fmt

# Check linter warnings
cargo clippy

# Fix common issues
cargo clippy --fix
```

## Contributing

When making changes:

1. Write tests for new functionality
2. Ensure all existing tests pass: `cargo test`
3. Run clippy: `cargo clippy`
4. Format code: `cargo fmt`
5. Document public APIs with doc comments
6. Update relevant documentation files

## Performance Considerations

### Lock Contention

- Use shortest critical sections possible
- Release locks explicitly with `.release().await` to avoid holding connections
- PostgreSQL connection pool size should account for lock overhead

### Database Pool Sizing

See [docs/configuration.md](README.md#connection-pool-sizing) for detailed pool sizing guidance.

### Lock Performance

- Memory locks are faster (no network round-trip)
- PostgreSQL locks have slight overhead but enable distributed coordination
- Choose based on deployment model (single-instance vs multi-instance HA)
