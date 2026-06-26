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
use futures_util::stream;

use crate::client::S3Client;
use crate::config::S3Config;

/// Default cap on entries returned from a single `query` when the caller does
/// not set [`LogQuery::limit`]. Keeps `daemon/logs` bounded to one round-trip
/// and avoids pulling an unbounded object set across the wire.
pub const DEFAULT_QUERY_LIMIT: usize = 500;

/// The S3 log-storage backend.
pub struct S3LogBackend {
    client: S3Client,
    prefix: String,
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
        let client = S3Client::connect(&cfg).await;
        Self { client, prefix }
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
            self.client
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

        let list_prefix = keys::list_prefix(&self.prefix, &filter);
        let mut all_keys = self
            .client
            .list_keys(&list_prefix)
            .await
            .map_err(|e| BackendError::Unavailable(e.to_string()))?;

        // Cheap pre-filter on the embedded timestamp before fetching bodies.
        all_keys.retain(|k| keys::key_in_time_window(k, filter.since, filter.until));

        // Newest keys carry the largest embedded millis; sort descending so we
        // fetch the most-recent window first and stop at the limit.
        all_keys.sort_by(|a, b| b.cmp(a));

        let limit = filter.limit.unwrap_or(DEFAULT_QUERY_LIMIT);

        let mut entries: Vec<LogEntry> = Vec::new();
        for key in all_keys {
            if entries.len() >= limit {
                break;
            }
            let bytes = match self.client.get_object(&key).await {
                Ok(b) => b,
                // A key listed-then-deleted (or otherwise unreadable) should
                // not fail the whole query — skip it.
                Err(_) => continue,
            };
            let entry: LogEntry = match serde_json::from_slice(&bytes) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if keys::matches(&entry, &filter) {
                entries.push(entry);
            }
        }

        // Return chronological (oldest first) per the protocol contract.
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
        match self.client.ping().await {
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
                last_error: Some(format!("s3 bucket {}: {err}", self.client.bucket())),
            }),
        }
    }
}
