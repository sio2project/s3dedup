#!/usr/bin/env python3
"""
Stress test for s3dedup PUT/GET/DELETE operations
"""

import argparse
import os
import random
import sys
import time
import urllib.parse
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime


def get_timestamp():
    """Generate RFC 2822 timestamp"""
    return datetime.now().strftime('%a, %d %b %Y %H:%M:%S +0000')


def encode_timestamp(ts):
    """URL encode timestamp"""
    return urllib.parse.quote(ts, safe='')


def put_file(base_url, path, content):
    """PUT a file to s3dedup"""
    timestamp = encode_timestamp(get_timestamp())
    url = f"{base_url}/ft/files{path}?last_modified={timestamp}"

    req = urllib.request.Request(url, data=content, method='PUT')
    req.add_header('Content-Type', 'application/octet-stream')

    try:
        response = urllib.request.urlopen(req, timeout=30)
        return response.status == 200
    except urllib.error.HTTPError as e:
        print(f"PUT {path} failed: HTTP {e.code}")
        return False
    except Exception as e:
        print(f"PUT {path} error: {e}")
        return False


def get_file(base_url, path):
    """GET a file from s3dedup"""
    url = f"{base_url}/ft/files{path}"

    try:
        response = urllib.request.urlopen(url, timeout=30)
        _ = response.read()  # consume body
        return response.status == 200
    except urllib.error.HTTPError as e:
        if e.code != 404:
            print(f"GET {path} failed: HTTP {e.code}")
        return False
    except Exception as e:
        print(f"GET {path} error: {e}")
        return False


def delete_file(base_url, path):
    """DELETE a file from s3dedup"""
    timestamp = encode_timestamp(get_timestamp())
    url = f"{base_url}/ft/files{path}?last_modified={timestamp}"

    req = urllib.request.Request(url, method='DELETE')

    try:
        response = urllib.request.urlopen(req, timeout=30)
        return response.status == 200
    except urllib.error.HTTPError as e:
        if e.code != 404:  # 404 is OK for cleanup
            print(f"DELETE {path} failed: HTTP {e.code}")
        return e.code == 404  # 404 is acceptable
    except Exception as e:
        print(f"DELETE {path} error: {e}")
        return False


def check_health(base_url):
    """Check if server is healthy"""
    try:
        response = urllib.request.urlopen(f"{base_url}/health", timeout=5)
        return response.status == 200
    except:
        return False


def generate_test_files(num_files, file_size):
    """Generate test file contents - 50% unique, 50% duplicates"""
    files = {}
    patterns = {}

    for i in range(1, num_files + 1):
        if i % 2 == 0:
            # Unique content
            files[i] = os.urandom(file_size)
        else:
            # Duplicate content based on pattern
            pattern_id = i % 10
            if pattern_id not in patterns:
                patterns[pattern_id] = f"pattern_{pattern_id}_".encode() + os.urandom(file_size - 20)
            files[i] = patterns[pattern_id]

    return files


def run_phase(name, tasks, executor, show_progress=True):
    """Run a phase of operations and report results"""
    print(f"\n=== {name} ===")
    start = time.time()

    futures = {executor.submit(task): idx for idx, task in enumerate(tasks)}
    success = 0
    errors = 0
    total = len(futures)
    completed = 0

    for future in as_completed(futures):
        completed += 1
        if future.result():
            success += 1
        else:
            errors += 1

        if show_progress and completed % max(1, total // 10) == 0:
            print(f"  Progress: {completed}/{total}")

    elapsed = time.time() - start
    rate = total / elapsed if elapsed > 0 else 0
    print(f"Completed: {total} ops in {elapsed:.2f}s ({rate:.2f} ops/sec), success: {success}, errors: {errors}")

    return success, errors


def main():
    parser = argparse.ArgumentParser(description='s3dedup stress test')
    parser.add_argument('--url', default='http://localhost:8080', help='Base URL')
    parser.add_argument('--files', type=int, default=100, help='Number of files')
    parser.add_argument('--parallel', type=int, default=10, help='Parallel workers')
    parser.add_argument('--size', type=int, default=1024, help='File size in bytes')
    parser.add_argument('--skip-cleanup', action='store_true', help='Skip cleanup phase')
    args = parser.parse_args()

    print("=== s3dedup Stress Test ===")
    print(f"Base URL: {args.url}")
    print(f"Number of files: {args.files}")
    print(f"Parallel workers: {args.parallel}")
    print(f"File size: {args.size} bytes")

    # Check server health
    print("\nChecking server health...")
    if not check_health(args.url):
        print(f"ERROR: Server not reachable at {args.url}")
        print("Start s3dedup with: cargo run -- server -c config.postgres.json")
        sys.exit(1)
    print("Server is healthy")

    # Generate test files
    print(f"\nGenerating {args.files} test files...")
    files = generate_test_files(args.files, args.size)
    print("Done")

    executor = ThreadPoolExecutor(max_workers=args.parallel)

    try:
        # Phase 1: PUT files
        tasks = [
            lambda i=i, content=content: put_file(args.url, f"/stress/file_{i}.bin", content)
            for i, content in files.items()
        ]
        run_phase(f"Phase 1: PUT {args.files} files", tasks, executor)

        # Phase 2: GET files
        tasks = [
            lambda i=i: get_file(args.url, f"/stress/file_{i}.bin")
            for i in files.keys()
        ]
        run_phase(f"Phase 2: GET {args.files} files", tasks, executor)

        # Phase 3: Mixed workload
        mixed_ops = args.files * 2
        def mixed_op(idx):
            op = random.randint(0, 3)
            file_num = random.randint(1, args.files)
            if op <= 1:  # 50% GET
                return get_file(args.url, f"/stress/file_{file_num}.bin")
            elif op == 2:  # 25% PUT update
                return put_file(args.url, f"/stress/file_{file_num}.bin", files[file_num])
            else:  # 25% new PUT
                return put_file(args.url, f"/stress/new_{idx}.bin", os.urandom(args.size))

        tasks = [lambda idx=i: mixed_op(idx) for i in range(mixed_ops)]
        run_phase(f"Phase 3: Mixed workload ({mixed_ops} ops)", tasks, executor)

        # Phase 4: Concurrent PUT/DELETE race test
        race_files = 50
        def race_op(idx):
            path = f"/stress/race_{idx}.bin"
            content = f"race_content_{idx}".encode()
            for _ in range(5):
                put_file(args.url, path, content)
                delete_file(args.url, path)
            return True

        tasks = [lambda idx=i: race_op(idx) for i in range(race_files)]
        run_phase(f"Phase 4: Race test ({race_files} files x 10 ops)", tasks, executor)

        # Phase 5: DELETE files (cleanup)
        if not args.skip_cleanup:
            tasks = [
                lambda i=i: delete_file(args.url, f"/stress/file_{i}.bin")
                for i in files.keys()
            ]
            run_phase(f"Phase 5: DELETE {args.files} files", tasks, executor)

            # Clean new files from mixed workload
            for i in range(mixed_ops):
                delete_file(args.url, f"/stress/new_{i}.bin")

    finally:
        executor.shutdown(wait=True)

    print("\n=== Stress Test Complete ===")


if __name__ == '__main__':
    main()
