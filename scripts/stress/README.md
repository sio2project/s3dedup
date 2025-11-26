# Stress Test

Stress testing tools for s3dedup.

## Setup

```bash
# Start infrastructure
docker compose up -d

# Start s3dedup with postgres config
cargo run -- server -c scripts/stress/config.postgres.json
```

## Run

```bash
python3 scripts/stress/stress_test.py
```

### Options

| Option | Default | Description |
|--------|---------|-------------|
| `--url` | http://localhost:8080 | Server URL |
| `--files` | 100 | Number of test files |
| `--parallel` | 10 | Concurrent workers |
| `--size` | 1024 | File size (bytes) |
| `--skip-cleanup` | false | Don't delete files after test |

### Example

```bash
python3 scripts/stress/stress_test.py --files 500 --parallel 20 --size 4096
```
