#[cfg(test)]
mod postgres_locks_tests {
    //! Integration tests for PostgreSQL-based distributed locks
    //!
    //! These tests verify that the PostgreSQL advisory locks implementation works correctly
    //! for both exclusive and shared lock scenarios in a distributed setting.
    //!
    //! NOTE: These tests require a running PostgreSQL instance with the DATABASE_URL environment variable set.
    //! If DATABASE_URL is not set, the tests are skipped.
    use s3dedup::config::{BucketConfig, Config, KVStorageType, MinIOConfig, PostgresConfig};
    use s3dedup::locks::{LocksStorage, LocksType};
    use std::sync::Arc;

    fn get_postgres_config() -> Option<Config> {
        // Only run PostgreSQL tests if DATABASE_URL is set
        if std::env::var("DATABASE_URL").is_err() {
            return None;
        }

        let bucket_config = BucketConfig {
            name: "test-postgres-locks".to_string(),
            address: "127.0.0.1".to_string(),
            port: 3001,
            s3storage_type: s3dedup::s3storage::S3StorageType::MinIO,
            minio: Some(MinIOConfig {
                endpoint: "http://localhost:9000".to_string(),
                access_key: "minioadmin".to_string(),
                secret_key: "minioadmin".to_string(),
                force_path_style: true,
            }),
            cleaner: s3dedup::cleaner::CleanerConfig::default(),
            filetracker_url: None,
            filetracker_v1_dir: None,
        };

        Some(Config {
            logging: s3dedup::logging::LoggingConfig {
                level: "info".to_string(),
                json: false,
            },
            kvstorage_type: KVStorageType::Postgres,
            sqlite: None,
            postgres: Some(PostgresConfig {
                host: "localhost".to_string(),
                port: 5432,
                user: "postgres".to_string(),
                password: "postgres".to_string(),
                dbname: "s3dedup_test".to_string(),
                pool_size: 10,
            }),
            locks_type: LocksType::Postgres,
            bucket: bucket_config,
        })
    }

    #[tokio::test]
    async fn test_postgres_locks_creation() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        // Should successfully create PostgreSQL locks
        let locks = LocksStorage::new_with_config(LocksType::Postgres, &config).await;
        assert!(
            locks.is_ok(),
            "Failed to create PostgreSQL locks: {:?}",
            locks.err()
        );
    }

    #[tokio::test]
    async fn test_exclusive_lock_mutual_exclusion() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        let locks = match LocksStorage::new_with_config(LocksType::Postgres, &config).await {
            Ok(l) => l,
            Err(e) => {
                panic!("Failed to create locks: {}", e);
            }
        };

        let locks = Arc::new(locks);
        let lock_key = "test:exclusive:key".to_string();

        // First exclusive lock should acquire successfully
        let lock1 = locks.prepare_lock(lock_key.clone()).await;
        let guard1 = lock1.acquire_exclusive().await;

        // Spawn a task to try to acquire the same lock
        let locks_for_task = locks.clone();
        let lock_key_clone = lock_key.clone();

        let task = tokio::spawn(async move {
            // This should block until guard1 is released
            let lock2 = locks_for_task.prepare_lock(lock_key_clone).await;
            let _guard2 = lock2.acquire_exclusive().await;
            // If we get here, the lock was acquired (after guard1 was dropped)
            true
        });

        // Give the task time to start and attempt to acquire the lock
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The task should still be waiting (not completed)
        assert!(
            !task.is_finished(),
            "Lock should be held and task should be waiting"
        );

        // Drop the first lock
        drop(guard1);

        // Now the task should be able to acquire the lock
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("Task should complete within 5 seconds")
            .expect("Task should complete successfully");

        assert!(result, "Second lock should have been acquired");
    }

    #[tokio::test]
    async fn test_shared_locks_concurrent() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        let locks = match LocksStorage::new_with_config(LocksType::Postgres, &config).await {
            Ok(l) => l,
            Err(e) => {
                panic!("Failed to create locks: {}", e);
            }
        };

        let locks = Arc::new(locks);
        let lock_key = "test:shared:key".to_string();

        // Multiple shared locks on the same key should be able to coexist
        let lock1 = locks.prepare_lock(lock_key.clone()).await;
        let lock2 = locks.prepare_lock(lock_key.clone()).await;

        let guard1 = lock1.acquire_shared().await;
        let guard2 = lock2.acquire_shared().await;

        // Both guards are held - this should not deadlock
        drop(guard1);
        drop(guard2);
    }

    #[tokio::test]
    async fn test_exclusive_blocks_shared() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        let locks = match LocksStorage::new_with_config(LocksType::Postgres, &config).await {
            Ok(l) => l,
            Err(e) => {
                panic!("Failed to create locks: {}", e);
            }
        };

        let locks = Arc::new(locks);
        let lock_key = "test:exclusive-shared:key".to_string();

        // Acquire an exclusive lock
        let lock1 = locks.prepare_lock(lock_key.clone()).await;
        let guard1 = lock1.acquire_exclusive().await;

        // Try to acquire a shared lock in another task
        let locks_clone = locks.clone();
        let lock_key_clone = lock_key.clone();

        let task = tokio::spawn(async move {
            let lock2 = locks_clone.prepare_lock(lock_key_clone).await;
            let _guard2 = lock2.acquire_shared().await;
            true
        });

        // Give the task time to attempt to acquire the lock
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // The task should still be waiting (not completed)
        assert!(
            !task.is_finished(),
            "Shared lock should be blocked by exclusive lock"
        );

        // Drop the exclusive lock
        drop(guard1);

        // Now the task should be able to acquire the shared lock
        let result = tokio::time::timeout(std::time::Duration::from_secs(5), task)
            .await
            .expect("Task should complete within 5 seconds")
            .expect("Task should complete successfully");

        assert!(
            result,
            "Shared lock should have been acquired after exclusive lock released"
        );
    }

    #[tokio::test]
    async fn test_different_keys_independent() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        let locks = match LocksStorage::new_with_config(LocksType::Postgres, &config).await {
            Ok(l) => l,
            Err(e) => {
                panic!("Failed to create locks: {}", e);
            }
        };

        let lock_key1 = "test:key:1".to_string();
        let lock_key2 = "test:key:2".to_string();

        // Acquire locks on different keys
        let lock1 = locks.prepare_lock(lock_key1).await;
        let lock2 = locks.prepare_lock(lock_key2).await;

        let guard1 = lock1.acquire_exclusive().await;

        // Should be able to acquire exclusive lock on different key immediately
        let guard2 = lock2.acquire_exclusive().await;

        // Both locks should be held independently
        drop(guard1);
        drop(guard2);
    }

    #[tokio::test]
    async fn test_lock_release_on_guard_drop() {
        let config = match get_postgres_config() {
            Some(c) => c,
            None => {
                println!("Skipping PostgreSQL locks tests - DATABASE_URL not set");
                return;
            }
        };

        let locks = match LocksStorage::new_with_config(LocksType::Postgres, &config).await {
            Ok(l) => l,
            Err(e) => {
                panic!("Failed to create locks: {}", e);
            }
        };

        let locks = Arc::new(locks);
        let lock_key = "test:release:key".to_string();

        // Acquire and release lock in a scope
        {
            let lock1 = locks.prepare_lock(lock_key.clone()).await;
            let _guard1 = lock1.acquire_exclusive().await;
            // Guard is dropped here
        }

        // Give time for the background task to release the lock
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // Should be able to acquire the lock immediately
        let lock2 = locks.prepare_lock(lock_key.clone()).await;
        let guard2 = lock2.acquire_exclusive().await;

        // If we get here, the lock was successfully released and reacquired
        drop(guard2);
    }
}
