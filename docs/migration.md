# Migration Guide

This guide covers migrating from Filetracker v2.1+ to s3dedup.

> **Note**: Migration from Filetracker v1.x will be covered in a future version of this guide.

## Overview

s3dedup provides two migration strategies:

1. **Offline Migration**: Migrate all files while the system is offline (recommended for smaller datasets)
2. **Live Migration**: Migrate while serving traffic with zero downtime (recommended for production systems)

Both strategies preserve:
- File content and metadata
- File paths and directory structure
- Last-modified timestamps
- Compression (gzip)

## Prerequisites

Before starting migration:

1. **Backup**: Create a backup of your Filetracker data
2. **S3 Storage**: Ensure S3/MinIO is accessible and configured
3. **Database**: Set up PostgreSQL or SQLite for metadata storage
4. **Network**: Verify connectivity between s3dedup and old Filetracker server
5. **Capacity**: Ensure sufficient S3 storage (deduplication will reduce actual usage)

## Migration Strategy Comparison

| Feature | Offline Migration | Live Migration |
|---------|------------------|----------------|
| Downtime | Required | Zero downtime |
| Complexity | Simple | More complex |
| Migration Speed | Faster | Slower (serves traffic) |
| Rollback | Easy | Moderate |
| Best For | Dev/Test, Small datasets | Production, Large datasets |

## Offline Migration

### Step 1: Prepare Configuration

Create a `.env` file with your configuration:

```bash
# S3 Configuration
S3_ENDPOINT=http://minio:9000
S3_ACCESS_KEY=minioadmin
S3_SECRET_KEY=minioadmin
S3_FORCE_PATH_STYLE=true

# Storage Configuration
KVSTORAGE_TYPE=postgres
POSTGRES_HOST=postgres
POSTGRES_PORT=5432
POSTGRES_USER=s3dedup
POSTGRES_PASSWORD=your_password
POSTGRES_DB=s3dedup
POSTGRES_MAX_CONNECTIONS=20

# Bucket Configuration
BUCKET_NAME=filetracker-bucket
LISTEN_ADDRESS=0.0.0.0
LISTEN_PORT=8080

# Logging
LOG_LEVEL=info
LOG_JSON=false
```

### Step 2: Stop Old Filetracker

```bash
# Stop your old Filetracker server to prevent writes during migration
systemctl stop filetracker
# or
docker stop filetracker
```

### Step 3: Run Offline Migration

```bash
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  migrate --env \
  --filetracker-url http://old-filetracker:8000 \
  --max-concurrency 10
```

#### Migration Options

- `--filetracker-url`: URL of your old Filetracker server
- `--max-concurrency`: Number of parallel file transfers (default: 10)
  - Increase for faster migration (20-50 for good network/storage)
  - Decrease if experiencing resource constraints

#### Monitoring Progress

The migration command outputs:

```
[INFO] Starting offline migration from http://old-filetracker:8000
[INFO] Fetching file list...
[INFO] Found 15,432 files to migrate
[INFO] Progress: 1000/15432 (6.5%) - 45 files/sec - ETA: 5m 20s
[INFO] Progress: 2000/15432 (13.0%) - 48 files/sec - ETA: 4m 40s
...
[INFO] Migration complete: 15,432 files migrated, 8,234 unique blobs stored (46.7% deduplication)
[INFO] Total storage: 142.3 GB → 75.8 GB (46.7% savings)
```

### Step 4: Verify Migration

Check that files were migrated successfully:

```bash
# Start s3dedup
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest

# Test some files
curl -I http://localhost:8080/ft/files/path/to/test-file.txt
curl http://localhost:8080/ft/files/path/to/test-file.txt -o test-file.txt

# Compare with original
diff test-file.txt /path/to/original/test-file.txt
```

### Step 5: Update Clients

Update your application configuration to point to the new s3dedup server:

```bash
# Old configuration
FILETRACKER_URL=http://old-filetracker:8000

# New configuration
FILETRACKER_URL=http://s3dedup:8080
```

### Step 6: Decommission Old Server

Once verified, you can decommission the old Filetracker server:

```bash
# Keep backup for a grace period (e.g., 30 days)
# Then remove old Filetracker installation
```

## Live Migration

Live migration allows zero-downtime migration by running s3dedup alongside your existing Filetracker server.

### Architecture

```
┌─────────────┐
│   Clients   │
└──────┬──────┘
       │
       ▼
┌─────────────────────┐
│     s3dedup         │
│  (live-migrate)     │
└─────┬───────┬───────┘
      │       │
      │       └──────────┐
      ▼                  ▼
┌──────────┐    ┌────────────────┐
│   S3     │    │ Old Filetracker│
│ (primary)│    │   (fallback)   │
└──────────┘    └────────────────┘
```

### Step 1: Prepare Configuration

Same as offline migration, but add:

```bash
# Add Filetracker fallback URL
FILETRACKER_URL=http://old-filetracker:8000
```

### Step 2: Start Live Migration

```bash
docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  live-migrate --env --max-concurrency 10
```

### How Live Migration Works

#### PUT Requests (Dual-Write)

```
Client uploads file → s3dedup writes to:
                      ├─ S3 + metadata (primary)
                      └─ Old Filetracker (for safety)
```

Files uploaded during migration are immediately available in both systems.

#### GET Requests (Fallback + Migration)

```
Client requests file → s3dedup:
                       ├─ Check local metadata
                       │  └─ Found? Return from S3
                       └─ Not found?
                          └─ Fetch from old Filetracker
                             └─ Migrate to S3 on-the-fly
                                └─ Return to client
```

Files are migrated lazily as they're accessed.

#### DELETE Requests (Dual-Delete)

```
Client deletes file → s3dedup:
                      ├─ Delete from S3 + metadata
                      └─ Also delete from old Filetracker
```

### Step 3: Monitor Migration Progress

Watch the logs:

```bash
docker logs -f s3dedup
```

You'll see:

```
[INFO] Starting live migration mode
[INFO] Filetracker fallback enabled: http://old-filetracker:8000
[INFO] Background migration started with concurrency=10
[INFO] Server listening on 0.0.0.0:8080
[INFO] Background: Migrated 1000 files (6.5%) - 42 files/sec
[INFO] On-demand: Migrated /project/rarely-accessed/file.pdf
[INFO] Background: Migrated 2000 files (13.0%) - 45 files/sec
```

### Step 4: Monitor Metrics

Check migration progress via API:

```bash
# Get migration status (if implemented)
curl http://localhost:8080/admin/migration/status

# Sample response:
{
  "mode": "live",
  "total_files": 15432,
  "migrated_files": 8234,
  "progress_percent": 53.4,
  "deduplication_rate": 46.7,
  "storage_saved_gb": 66.5
}
```

### Step 5: Gradual Traffic Migration

Gradually shift traffic to s3dedup:

```bash
# Week 1: 10% of clients point to s3dedup
# Week 2: 50% of clients point to s3dedup
# Week 3: 100% of clients point to s3dedup
```

This allows you to verify behavior before full cutover.

### Step 6: Complete Background Migration

Wait for background migration to complete (or force completion):

```bash
# Check migration status
docker logs s3dedup | grep "Background migration"

# When complete:
[INFO] Background migration complete: 15,432/15,432 files (100%)
```

### Step 7: Switch to Normal Mode

Once all files are migrated and traffic is stable:

1. Remove `FILETRACKER_URL` from configuration
2. Restart s3dedup in normal mode:

```bash
# Update .env (remove FILETRACKER_URL)
# Restart container
docker stop s3dedup
docker rm s3dedup

docker run -d \
  --name s3dedup \
  -p 8080:8080 \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest
```

### Step 8: Decommission Old Server

After a grace period (e.g., 7-30 days), decommission old Filetracker.

## Migration Performance

### Expected Transfer Rates

| Network | Storage | Files/sec | GB/hour |
|---------|---------|-----------|---------|
| 1 Gbps | HDD | 50-100 | 10-20 |
| 1 Gbps | SSD | 100-200 | 20-40 |
| 10 Gbps | SSD | 500-1000 | 100-200 |

Actual rates depend on:
- Average file size (smaller files = lower throughput)
- Network latency
- S3 backend performance
- Database performance
- Concurrency settings

### Tuning Performance

#### Increase Concurrency

```bash
# For high-performance networks and storage
--max-concurrency 50
```

#### Optimize Database

PostgreSQL settings for migration:

```sql
-- Increase connections for concurrent workers
max_connections = 100

-- Optimize for bulk writes
shared_buffers = 256MB
work_mem = 16MB
maintenance_work_mem = 128MB

-- Reduce fsync during migration (restore after)
synchronous_commit = off
```

#### Network Optimization

- Place s3dedup close to S3 storage (same datacenter/region)
- Use high-bandwidth network links
- Consider compression (already enabled for gzip content)

## Troubleshooting

### Migration Stalls

**Symptom**: Migration progress stops

**Causes**:
- Network connectivity issues
- S3 backend overloaded
- Database connection exhaustion

**Solutions**:
```bash
# Check logs
docker logs s3dedup | tail -100

# Reduce concurrency
--max-concurrency 5

# Check connectivity
curl http://old-filetracker:8000/ft/version
```

### Files Not Found After Migration

**Symptom**: GET returns 404 for migrated files

**Causes**:
- Migration incomplete
- Path encoding differences
- Database corruption

**Solutions**:
```bash
# Check file in database
docker exec -it postgres psql -U s3dedup -d s3dedup \
  -c "SELECT * FROM files WHERE path LIKE '%filename%';"

# Re-migrate specific file
curl -X PUT http://s3dedup:8080/ft/files/path/to/file.txt \
  --data-binary @file.txt
```

### High Memory Usage During Migration

**Symptom**: s3dedup consumes excessive memory

**Causes**:
- Too many concurrent workers
- Large files being buffered

**Solutions**:
```bash
# Reduce concurrency
--max-concurrency 5

# Set container memory limits
docker run --memory=2g ...
```

### Database Lock Timeouts

**Symptom**: `database is locked` errors (SQLite)

**Solutions**:
```bash
# Switch to PostgreSQL for migration
KVSTORAGE_TYPE=postgres

# Or reduce concurrency for SQLite
--max-concurrency 1
```

## Rollback Procedures

### During Offline Migration

If migration fails:

1. Keep old Filetracker data intact
2. Fix issues with s3dedup configuration
3. Re-run migration (idempotent - safe to retry)

```bash
# Clear partial migration
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  admin clear-bucket --confirm

# Restart migration
docker run --rm \
  --env-file .env \
  -v s3dedup-data:/app/data \
  ghcr.io/sio2project/s3dedup:latest \
  migrate --env --filetracker-url http://old-filetracker:8000
```

### During Live Migration

If issues occur during live migration:

1. Stop s3dedup
2. Point all clients back to old Filetracker
3. Investigate and fix issues
4. Restart live migration

```bash
# Emergency rollback
docker stop s3dedup

# Clients automatically use old Filetracker
# No data loss - dual-writes kept everything in sync

# Fix issues and restart
docker start s3dedup
```

## Post-Migration Tasks

### 1. Enable Background Cleaner

After migration, enable the cleaner to remove orphaned blobs:

```bash
# Add to .env
CLEANER_ENABLED=true
CLEANER_INTERVAL=3600
CLEANER_BATCH_SIZE=1000
```

### 2. Monitor Storage Savings

```bash
# Check deduplication metrics
curl http://localhost:8080/admin/stats

# Sample response:
{
  "total_files": 15432,
  "unique_blobs": 8234,
  "deduplication_rate": 46.7,
  "logical_storage_gb": 142.3,
  "physical_storage_gb": 75.8,
  "savings_gb": 66.5
}
```

### 3. Optimize Database

After migration, optimize the database:

```sql
-- PostgreSQL
VACUUM ANALYZE;
REINDEX DATABASE s3dedup;

-- Update statistics
ANALYZE files;
ANALYZE refcounts;
ANALYZE sizes;
```

### 4. Backup Configuration

Back up your configuration:

```bash
# Backup database
pg_dump -U s3dedup s3dedup > s3dedup-backup.sql

# Backup config
cp .env .env.backup
```

## Migration Checklist

### Pre-Migration

- [ ] Backup old Filetracker data
- [ ] Set up S3/MinIO storage
- [ ] Set up PostgreSQL/SQLite database
- [ ] Test connectivity between s3dedup and Filetracker
- [ ] Verify sufficient storage capacity
- [ ] Choose migration strategy (offline vs live)

### During Migration

- [ ] Monitor migration progress
- [ ] Watch for errors in logs
- [ ] Verify file integrity (sample files)
- [ ] Monitor system resources (CPU, memory, network)

### Post-Migration

- [ ] Verify all critical files migrated
- [ ] Test client applications
- [ ] Monitor deduplication metrics
- [ ] Enable background cleaner
- [ ] Optimize database
- [ ] Update documentation
- [ ] Plan decommissioning of old server

## Support

If you encounter issues during migration:

1. Check logs: `docker logs s3dedup`
2. Review troubleshooting section above
3. Check GitHub issues: https://github.com/sio2project/s3dedup/issues
4. File a new issue with:
   - Migration strategy used
   - Error messages from logs
   - Database type and version
   - S3 backend type and version
   - Approximate data size
