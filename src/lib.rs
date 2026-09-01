//! S3-compatible `log_storage_backend` for Animus.
//!
//! Persists [`LogEntry`] records as individual JSON objects in an
//! S3-compatible bucket so agent run logs survive past the lifetime of an
//! ephemeral container disk. Works against any S3 API — Railway buckets,
//! MinIO, Cloudflare R2, AWS S3 — via an endpoint URL + path-style addressing.
//!
//! See [`keys`] for the object-key layout and the query/filter strategy.

pub mod client;
pub mod config;
pub mod keys;

use animus_log_storage_protocol::{
    BackendError, LogEntry, LogQuery, LogQueryResult, LogStorageSchema, LogStream,
    SupportsFiltering,
};
use animus_plugin_protocol::{HealthCheckResult, HealthStatus};
use async_trait::async_trait;
use futures_util::{stream, StreamExt};
use std::sync::Arc;

use crate::client::{ObjectStore, S3Client};
use crate::config::S3Config;

/// Default cap on entries returned from a single `query` when the caller does
/// not set [`LogQuery::limit`]. Keeps `daemon/logs` bounded to one round-trip
/// and avoids pulling an unbounded object set across the wire.
pub const DEFAULT_QUERY_LIMIT: usize = 500;

/// Maximum number of object bodies fetched concurrently while resolving a
/// query. Object storage GETs are latency-bound, so fanning out turns a long
/// sequential chain into a handful of round-trips; the cap bounds memory and
/// the number of in-flight connections.
const FETCH_CONCURRENCY: usize = 32;

/// Fetch and decode a single stored entry, returning `None` when the object is
/// missing (listed-then-deleted) or its body is not a valid [`LogEntry`] — a
/// single bad object must never fail the whole query.
async fn fetch_entry(store: &dyn ObjectStore, key: &str) -> Option<LogEntry> {
    let bytes = store.get_object(key).await.ok()?;
    serde_json::from_slice(&bytes).ok()
}

/// The S3 log-storage backend.
pub struct S3LogBackend {
    store: Arc<dyn ObjectStore>,
    prefix: String,
    /// Bucket label for health reporting; `None` for custom stores.
    bucket: Option<String>,
}

impl S3LogBackend {
    /// Build the backend from environment-sourced configuration.
    pub async fn from_env() -> anyhow::Result<Self> {
        let cfg = S3Config::from_env()?;
        Ok(Self::from_config(cfg).await)
    }

    /// Build the backend from an explicit configuration.
    pub async fn from_config(cfg: S3Config) -> Self {
        let prefix = cfg.prefix.clone();
        let bucket = cfg.bucket.clone();
        let client = S3Client::connect(&cfg).await;
        Self {
            store: Arc::new(client),
            prefix,
            bucket: Some(bucket),
        }
    }

    /// Build the backend over an arbitrary [`ObjectStore`] implementation.
    /// Used by tests with an in-memory store, and by embedders that already
    /// hold a configured store.
    pub fn with_store(store: Arc<dyn ObjectStore>, prefix: String) -> Self {
        Self {
            store,
            prefix,
            bucket: None,
        }
    }
}

#[async_trait]
impl animus_log_storage_protocol::LogStorageBackend for S3LogBackend {
    async fn store(&self, entries: Vec<LogEntry>) -> Result<(), BackendError> {
        for entry in entries {
            let key = keys::entry_key(&self.prefix, &entry);
            let body = serde_json::to_vec(&entry).map_err(|e| {
                BackendError::Other(anyhow::anyhow!("encode entry {}: {e}", entry.id))
            })?;
            self.store
                .put_object(&key, body)
                .await
                .map_err(|e| BackendError::Unavailable(e.to_string()))?;
        }
        Ok(())
    }

    async fn query(&self, filter: LogQuery) -> Result<LogQueryResult, BackendError> {
        if let (Some(since), Some(until)) = (filter.since, filter.until) {
            if since > until {
                return Err(BackendError::InvalidRequest(
                    "since must not be after until".into(),
                ));
            }
        }

        let limit = filter.limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let mut entries: Vec<LogEntry> = Vec::new();
        if limit == 0 {
            return Ok(LogQueryResult {
                entries,
                next_cursor: None,
            });
        }

        // Scan the smallest set of prefixes that can hold matches, newest
        // window first. When source + source_name are pinned and a `since`
        // floor is present this addresses concrete day prefixes directly
        // instead of listing the whole source history (see `keys::scan_prefixes`).
        let store = self.store.as_ref();
        'scan: for list_prefix in keys::scan_prefixes(&self.prefix, &filter) {
            let mut keys = store
                .list_keys(&list_prefix)
                .await
                .map_err(|e| BackendError::Unavailable(e.to_string()))?;

            // Cheap pre-filter on the embedded timestamp before fetching bodies.
            keys.retain(|k| keys::key_in_time_window(k, filter.since, filter.until));

            // Newest keys carry the largest embedded millis; descending order
            // means every key in an earlier batch is newer than every key in a
            // later one, so once we have `limit` matches we can stop — the
            // remaining keys are strictly older.
            keys.sort_by(|a, b| b.cmp(a));

            for batch in keys.chunks(FETCH_CONCURRENCY) {
                // Fan out the body GETs for this batch; filter after decoding
                // (level/glob/time bounds are not expressible in the key).
                let mut fetched: Vec<LogEntry> = stream::iter(batch.iter().cloned())
                    .map(|key| async move { fetch_entry(store, &key).await })
                    .buffer_unordered(FETCH_CONCURRENCY)
                    .filter_map(|opt| async move { opt })
                    .collect()
                    .await;
                fetched.retain(|entry| keys::matches(entry, &filter));
                entries.extend(fetched);
                if entries.len() >= limit {
                    break 'scan;
                }
            }
        }

        // Entries were gathered newest-first across prefixes/batches; keep the
        // newest `limit`, then return chronological (oldest first) per contract.
        entries.sort_by(|a, b| b.ts.cmp(&a.ts).then_with(|| b.id.cmp(&a.id)));
        entries.truncate(limit);
        keys::sort_chronological(&mut entries);
        Ok(LogQueryResult {
            entries,
            next_cursor: None,
        })
    }

    async fn tail(&self, filter: LogQuery) -> Result<LogStream, BackendError> {
        // Object storage has no native change feed. We offer a best-effort
        // tail: a one-shot snapshot of the current matching window delivered
        // as a finite stream. `follow = true` cannot be honored (no push), so
        // we still return the snapshot rather than erroring — the daemon's
        // historical path uses `query` anyway.
        let result = self.query(filter).await?;
        let items: Vec<Result<LogEntry, BackendError>> =
            result.entries.into_iter().map(Ok).collect();
        Ok(Box::pin(stream::iter(items)))
    }

    fn schema(&self) -> LogStorageSchema {
        LogStorageSchema {
            supports_query: true,
            // Best-effort snapshot tail (no live follow over object storage).
            supports_tail: true,
            // Same `(ts, id)` derives the same key, so a re-store overwrites
            // in place — at-least-once writes are idempotent.
            supports_dedup: true,
            supports_filtering: SupportsFiltering {
                // We narrow the list prefix by source segment server-side.
                by_source: true,
                // Everything below is honored, but evaluated in-process after
                // fetching bodies, so we declare them honestly as the daemon
                // may still want to apply them. We only advertise filters we
                // actually apply (which we do). by_level/by_time_range/by_glob
                // are applied in `matches`, by_target via the glob path.
                by_level: true,
                by_time_range: true,
                by_target: true,
                by_glob: true,
            },
            // No hard query window; bounded by retention only.
            max_query_window: None,
            // Retention is bucket-lifecycle-policy driven (operator-configured
            // on the S3 side), so we do not advertise a fixed hint.
            retention_hint: None,
        }
    }

    async fn health(&self) -> Result<HealthCheckResult, BackendError> {
        match self.store.ping().await {
            Ok(()) => Ok(HealthCheckResult {
                status: HealthStatus::Healthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: None,
            }),
            Err(err) => Ok(HealthCheckResult {
                status: HealthStatus::Unhealthy,
                uptime_ms: None,
                memory_usage_bytes: None,
                last_error: Some(match &self.bucket {
                    Some(bucket) => format!("s3 bucket {bucket}: {err}"),
                    None => format!("object store: {err}"),
                }),
            }),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use animus_log_storage_protocol::{LogLevel, LogSource, LogStorageBackend};
    use chrono::{DateTime, Utc};
    use std::collections::BTreeMap;
    use tokio::sync::Mutex;

    /// In-memory [`ObjectStore`] for exercising the store/query/tail read
    /// path without a live bucket. Records the list prefixes it was asked to
    /// scan so tests can assert prefix narrowing.
    #[derive(Default)]
    struct MemoryStore {
        objects: Mutex<BTreeMap<String, Vec<u8>>>,
        listed_prefixes: Mutex<Vec<String>>,
    }

    #[async_trait]
    impl ObjectStore for MemoryStore {
        async fn put_object(&self, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
            self.objects.lock().await.insert(key.to_string(), body);
            Ok(())
        }

        async fn get_object(&self, key: &str) -> anyhow::Result<Vec<u8>> {
            self.objects
                .lock()
                .await
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("no such key: {key}"))
        }

        async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            self.listed_prefixes.lock().await.push(prefix.to_string());
            let keys = self
                .objects
                .lock()
                .await
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect();
            Ok(keys)
        }
    }

    impl MemoryStore {
        async fn listed(&self) -> Vec<String> {
            self.listed_prefixes.lock().await.clone()
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    fn run_entry(
        id: &str,
        workflow_id: &str,
        run_id: &str,
        ts: DateTime<Utc>,
        text: &str,
    ) -> LogEntry {
        LogEntry {
            id: id.into(),
            ts,
            level: LogLevel::Info,
            source: LogSource::Workflow,
            source_name: Some(workflow_id.into()),
            target: "workflow.run.output_chunk".into(),
            message: text.into(),
            fields: serde_json::json!({
                "run_id": run_id,
                "workflow_id": workflow_id,
                "source_file": "events.jsonl",
                "run_event": { "kind": "output_chunk", "run_id": run_id, "text": text },
            }),
        }
    }

    fn backend(store: Arc<MemoryStore>) -> S3LogBackend {
        S3LogBackend::with_store(store, "animus".to_string())
    }

    #[tokio::test]
    async fn store_then_query_reads_run_entries_back() {
        let store = Arc::new(MemoryStore::default());
        let backend = backend(store.clone());
        let entries = vec![
            run_entry(
                "e1",
                "wf-1",
                "wf-1-build-0",
                ts("2026-09-01T10:00:00Z"),
                "first",
            ),
            run_entry(
                "e2",
                "wf-1",
                "wf-1-build-0",
                ts("2026-09-01T10:00:05Z"),
                "second",
            ),
            // A different workflow must not leak into the run's read-back.
            run_entry(
                "e3",
                "wf-2",
                "wf-2-build-0",
                ts("2026-09-01T10:00:03Z"),
                "other",
            ),
        ];
        backend.store(entries).await.unwrap();

        let result = backend
            .query(LogQuery {
                source: Some(LogSource::Workflow),
                source_name: Some("wf-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();

        let messages: Vec<&str> = result.entries.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["first", "second"]); // chronological, wf-1 only
                                                   // The scan listed only the workflow's narrowed prefix, never the bucket root.
        assert_eq!(
            store.listed().await,
            vec!["animus/workflow/wf-1/".to_string()]
        );
    }

    #[tokio::test]
    async fn query_keeps_newest_when_over_limit() {
        let store = Arc::new(MemoryStore::default());
        let backend = backend(store);
        let entries = vec![
            run_entry("e1", "wf-1", "r1", ts("2026-09-01T10:00:00Z"), "oldest"),
            run_entry("e2", "wf-1", "r1", ts("2026-09-01T10:00:01Z"), "middle"),
            run_entry("e3", "wf-1", "r1", ts("2026-09-01T10:00:02Z"), "newest"),
        ];
        backend.store(entries).await.unwrap();

        let result = backend
            .query(LogQuery {
                source: Some(LogSource::Workflow),
                source_name: Some("wf-1".into()),
                limit: Some(2),
                ..Default::default()
            })
            .await
            .unwrap();

        let messages: Vec<&str> = result.entries.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["middle", "newest"]);
    }

    #[tokio::test]
    async fn query_tolerates_corrupt_objects() {
        let store = Arc::new(MemoryStore::default());
        let backend = backend(store.clone());
        backend
            .store(vec![run_entry(
                "e1",
                "wf-1",
                "r1",
                ts("2026-09-01T10:00:00Z"),
                "good",
            )])
            .await
            .unwrap();
        // A non-LogEntry object sitting under the same prefix (foreign writer,
        // partial upload) must not fail the query.
        store
            .put_object(
                "animus/workflow/wf-1/2026/09/01/0000000000001-broken.json",
                b"not json".to_vec(),
            )
            .await
            .unwrap();

        let result = backend
            .query(LogQuery {
                source: Some(LogSource::Workflow),
                source_name: Some("wf-1".into()),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.entries.len(), 1);
        assert_eq!(result.entries[0].message, "good");
    }

    #[tokio::test]
    async fn query_applies_time_window() {
        let store = Arc::new(MemoryStore::default());
        let backend = backend(store);
        backend
            .store(vec![
                run_entry("e1", "wf-1", "r1", ts("2026-09-01T09:59:59Z"), "before"),
                run_entry("e2", "wf-1", "r1", ts("2026-09-01T10:00:00Z"), "inside"),
            ])
            .await
            .unwrap();

        let result = backend
            .query(LogQuery {
                source: Some(LogSource::Workflow),
                source_name: Some("wf-1".into()),
                since: Some(ts("2026-09-01T10:00:00Z")),
                until: Some(ts("2026-09-01T10:00:01Z")),
                ..Default::default()
            })
            .await
            .unwrap();
        let messages: Vec<&str> = result.entries.iter().map(|e| e.message.as_str()).collect();
        assert_eq!(messages, ["inside"]);
    }

    #[tokio::test]
    async fn query_rejects_inverted_window() {
        let backend = backend(Arc::new(MemoryStore::default()));
        let err = backend
            .query(LogQuery {
                since: Some(ts("2026-09-01T11:00:00Z")),
                until: Some(ts("2026-09-01T10:00:00Z")),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert!(matches!(err, BackendError::InvalidRequest(_)));
    }

    #[tokio::test]
    async fn tail_returns_snapshot_of_matching_window() {
        use futures_util::StreamExt as _;
        let store = Arc::new(MemoryStore::default());
        let backend = backend(store);
        backend
            .store(vec![
                run_entry("e1", "wf-1", "r1", ts("2026-09-01T10:00:00Z"), "a"),
                run_entry("e2", "wf-1", "r1", ts("2026-09-01T10:00:01Z"), "b"),
            ])
            .await
            .unwrap();

        let mut stream = backend
            .tail(LogQuery {
                source: Some(LogSource::Workflow),
                source_name: Some("wf-1".into()),
                follow: true, // cannot be honored over object storage; snapshot anyway
                ..Default::default()
            })
            .await
            .unwrap();
        let mut messages = Vec::new();
        while let Some(item) = stream.next().await {
            messages.push(item.unwrap().message);
        }
        assert_eq!(messages, ["a", "b"]);
    }

    #[tokio::test]
    async fn health_reports_unhealthy_when_store_fails() {
        struct FailingStore;
        #[async_trait]
        impl ObjectStore for FailingStore {
            async fn put_object(&self, _key: &str, _body: Vec<u8>) -> anyhow::Result<()> {
                anyhow::bail!("nope")
            }
            async fn get_object(&self, _key: &str) -> anyhow::Result<Vec<u8>> {
                anyhow::bail!("nope")
            }
            async fn list_keys(&self, _prefix: &str) -> anyhow::Result<Vec<String>> {
                anyhow::bail!("bucket unreachable")
            }
        }
        let backend = S3LogBackend::with_store(Arc::new(FailingStore), String::new());
        let health = backend.health().await.unwrap();
        assert!(matches!(health.status, HealthStatus::Unhealthy));
        assert!(health.last_error.unwrap().contains("bucket unreachable"));
    }
}
