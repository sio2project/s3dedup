# Deduplication Architecture

## Overview

s3dedup is an S3 proxy that implements content-based deduplication while maintaining backward compatibility with the Filetracker protocol. It stores files in S3 by their content hash, allowing identical files to share the same underlying storage blob.

## Core Principle: Content-Addressable Storage

Instead of storing files by their logical path, s3dedup uses **content-addressable storage**:

```
Logical Path → SHA256 Hash → S3 Object
/path/to/file.txt → abc123... → s3://bucket/abc123...
```

When multiple files have identical content, they all point to the same S3 object.

## Data Flow

### PUT Operation (Upload)

```
1. Client uploads file to /ft/files/project/data.txt
   ↓
2. Decompress if needed (gzip)
   ↓
3. Compute SHA256 hash of uncompressed content
   hash = "abc123def456..."
   ↓
4. Acquire exclusive lock for this file path
   ↓
5. Check if blob exists in S3 (by hash)
   ├─ YES: Reuse existing blob
   └─ NO:  Upload compressed content to S3 as "abc123def456..."
   ↓
6. Update metadata in KV storage:
   - files table:     /project/data.txt → hash: abc123..., modified: 1234567890
   - refcounts table: abc123... → count: +1
   - sizes table:     abc123... → logical_size: 1024
   ↓
7. Handle old version (if overwriting):
   - Decrement old hash's refcount
   - Delete old blob from S3 if refcount = 0
   ↓
8. Release lock
```

### GET Operation (Download)

```
1. Client requests /ft/files/project/data.txt
   ↓
2. Acquire shared lock for this file path
   ↓
3. Look up file in KV storage:
   /project/data.txt → hash: abc123..., modified: 1234567890
   ↓
4. Look up logical size:
   abc123... → logical_size: 1024
   ↓
5. Fetch blob from S3:
   GET s3://bucket/abc123...
   ↓
6. Return file with headers:
   Content-Encoding: gzip
   Logical-Size: 1024
   Last-Modified: RFC2822 timestamp
   ↓
7. Release lock
```

### DELETE Operation

```
1. Client deletes /ft/files/project/data.txt
   ↓
2. Acquire exclusive lock for this file path
   ↓
3. Look up current hash for this path
   /project/data.txt → abc123...
   ↓
4. Delete file metadata:
   - files table: Remove /project/data.txt
   ↓
5. Decrement reference count:
   refcounts[abc123...] -= 1
   ↓
6. If refcount reaches 0:
   - Delete blob from S3: s3://bucket/abc123...
   - Delete size metadata: sizes[abc123...]
   - Delete refcount entry: refcounts[abc123...]
   ↓
7. Release lock
```

## Reference Counting

The reference counting system ensures blobs are only deleted when no files reference them:

```
Hash: abc123def456...
├─ /project/file1.txt
├─ /project/file2.txt
└─ /backup/copy.txt

Refcount: 3

When /project/file1.txt is deleted:
Refcount: 3 → 2 (blob kept)

When /backup/copy.txt is deleted:
Refcount: 2 → 1 (blob kept)

When /project/file2.txt is deleted:
Refcount: 1 → 0 (blob deleted from S3)
```

## Deduplication Example

### Scenario: Uploading Identical Files

```bash
# Upload file 1
PUT /ft/files/project1/document.pdf
Content: <1MB PDF>
SHA256: aabbccdd...

→ Stores: s3://bucket/aabbccdd...
→ Metadata: project1/document.pdf → aabbccdd..., refcount=1
→ Storage used: 1MB

# Upload file 2 with identical content
PUT /ft/files/project2/same-doc.pdf
Content: <same 1MB PDF>
SHA256: aabbccdd... (same hash!)

→ S3 blob already exists (reused)
→ Metadata: project2/same-doc.pdf → aabbccdd..., refcount=2
→ Storage used: 1MB (still!)

# Upload file 3 with different content
PUT /ft/files/project1/other.pdf
Content: <different 1MB PDF>
SHA256: eeffgghh...

→ Stores: s3://bucket/eeffgghh...
→ Metadata: project1/other.pdf → eeffgghh..., refcount=1
→ Storage used: 2MB total

Result:
- 3 logical files
- 2 physical S3 objects
- 50% deduplication rate
```

## Storage Layout

### S3 Bucket

```
s3://bucket/
├── abc123def456...  (blob: compressed content)
├── 789fedcba012...  (blob: compressed content)
└── ... (all blobs stored by SHA256 hash)
```

### KV Storage (PostgreSQL/SQLite)

#### files table
```sql
bucket    | path                  | modified    | hash
----------+-----------------------+-------------+----------------
mybucket  | /project/data.txt     | 1234567890  | abc123def456...
mybucket  | /backup/data.txt      | 1234567891  | abc123def456...
mybucket  | /project/other.txt    | 1234567892  | 789fedcba012...
```

#### refcounts table
```sql
bucket    | hash              | count
----------+-------------------+-------
mybucket  | abc123def456...   | 2      (referenced by 2 files)
mybucket  | 789fedcba012...   | 1      (referenced by 1 file)
```

#### sizes table
```sql
bucket    | hash              | logical_size
----------+-------------------+-------------
mybucket  | abc123def456...   | 1024
mybucket  | 789fedcba012...   | 2048
```

## Concurrency Control

File-level locking ensures consistency during concurrent operations:

### Lock Types

- **Shared Lock (read)**: Multiple readers can read the same file simultaneously
- **Exclusive Lock (write)**: Only one writer can modify a file at a time

### Lock Implementation

```rust
// Hybrid approach for optimal performance:
// - HashMap management: parking_lot::RwLock (sync, ~100ns)
// - File coordination: tokio::sync::RwLock (async, for I/O)

// PUT/DELETE operations
let lock = locks.prepare_lock(file_lock(bucket, path));
let _guard = lock.acquire_exclusive().await;
// ... perform write operation ...
// Guard drops, lock released

// GET operations
let lock = locks.prepare_lock(file_lock(bucket, path));
let _guard = lock.acquire_shared().await;
// ... perform read operation ...
// Guard drops, lock released
```

### Critical Sections Protected

1. **Reference count updates**: Prevents race conditions when multiple operations modify the same blob's refcount
2. **File overwrites**: Ensures atomic replacement of old hash with new hash
3. **Blob deletion**: Prevents deletion of blobs still being referenced

## Compression

All files are stored compressed in S3:

```
Client uploads → Decompress (if gzip) → Compute hash → Compress → Store in S3
Client downloads ← Return gzipped ← Fetch from S3
```

This provides:
- Additional storage savings (typically 50-70% for text files)
- Faster network transfer
- Consistent storage format

## Deduplication Efficiency

Deduplication works best when:

1. **Identical files across projects**: Build artifacts, dependencies, assets
2. **Versioned content with few changes**: Git repositories, backups
3. **Template-based files**: Generated reports, documents from templates

Example deduplication rates observed:
- **Source code repositories**: 30-50% (common dependencies)
- **Build artifacts**: 60-80% (identical binaries across builds)
- **User uploads**: 10-30% (varies by use case)

## Comparison with Traditional Storage

### Traditional File Storage
```
Path: /project1/file.txt → S3: /project1/file.txt (1MB)
Path: /project2/file.txt → S3: /project2/file.txt (1MB)
Total: 2MB stored
```

### s3dedup (Content-Addressable)
```
Path: /project1/file.txt → Hash: abc123... → S3: abc123... (1MB)
Path: /project2/file.txt → Hash: abc123... → S3: abc123... (reused)
Total: 1MB stored (50% savings)
```

## Edge Cases

### Race Condition: Concurrent PUT of Same Content

```
Thread A: PUT /file1.txt (hash: abc123...)
Thread B: PUT /file2.txt (hash: abc123...)

Both threads:
1. Check if blob exists → NO
2. Upload blob to S3 → Both succeed (S3 allows overwrites)
3. Increment refcount → Properly synchronized
4. Store file metadata → Each updates different paths

Result: ✓ Safe (last write wins in S3, refcount correct)
```

### Race Condition: Concurrent DELETE

```
Thread A: DELETE /file1.txt (hash: abc123..., current refcount: 2)
Thread B: DELETE /file2.txt (hash: abc123..., current refcount: 2)

Both threads (with locks):
1. Acquire exclusive lock for their path
2. Decrement refcount (synchronized)
   - Thread A: 2 → 1
   - Thread B: 1 → 0
3. Thread B sees refcount=0, deletes blob
4. Thread A sees refcount=1, keeps blob

Result: ✓ Safe (only deleted when truly unreferenced)
```

## Migration Support

s3dedup supports live migration from old Filetracker:

### Dual-Write Mode
```
PUT request →
├─ Write to s3dedup (primary)
└─ Also write to old Filetracker (for rollback safety)
```

### Fallback Read
```
GET request →
├─ Check s3dedup first
├─ If not found, check old Filetracker
└─ If found in Filetracker, migrate on-the-fly to s3dedup
```

This enables zero-downtime migration with automatic backfilling.

## Performance Characteristics

### Storage Efficiency
- **Space savings**: 30-80% depending on content duplication
- **Overhead**: Small metadata per file (~100 bytes in KV storage)

### Latency
- **PUT (new content)**: +hash computation (~10ms/MB) + S3 upload
- **PUT (duplicate)**: +hash computation (~10ms/MB), no S3 upload (faster!)
- **GET**: Same as normal S3 (one metadata lookup + S3 fetch)
- **DELETE**: +refcount update, S3 delete only if refcount=0

### Scalability
- **Horizontal**: Each bucket runs independently, can scale across servers
- **Storage**: Limited only by S3 and KV storage capacity
- **Throughput**: Lock-free reads, per-file locking for writes
