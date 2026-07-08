//! `animus-log-storage-s3` binary: stdio JSON-RPC `log_storage_backend`
//! plugin that persists Animus log entries to an S3-compatible bucket.
//!
//! Built on `animus_plugin_runtime::Plugin`, the generic stdio shell that
//! owns framing, the `initialize`/`shutdown` lifecycle, error envelopes, and
//! notification fan-out. We register the four `log_storage/*` methods on top.
//!
//! Wire shapes (per `animus-log-storage-protocol` + the kernel client in
//! `orchestrator-daemon-runtime/src/log_storage.rs`):
//!
//! - `log_storage/store`  — params `{ "entries": [LogEntry, ...] }` → `{}`
//! - `log_storage/query`  — params `LogQuery` → `LogQueryResult`
//! - `log_storage/tail`   — params `LogQuery` → `{}` ack, then a stream of
//!   `log_storage/event` notifications `{ id, entry }`, terminated by
//!   `{ id, done: true }`.
//! - `log_storage/schema` — params `{}` → `LogStorageSchema`

use std::sync::Arc;

use animus_log_storage_protocol::{
    LogEntry, LogQuery, LogStorageBackend, METHOD_LOG_STORAGE_QUERY, METHOD_LOG_STORAGE_SCHEMA,
    METHOD_LOG_STORAGE_STORE, METHOD_LOG_STORAGE_TAIL, NOTIFICATION_LOG_STORAGE_EVENT,
    PLUGIN_KIND_LOG_STORAGE_BACKEND,
};
use animus_plugin_protocol::{error_codes, EnvRequirement, RpcError};
use animus_plugin_runtime::{MethodContext, Plugin};
use serde::Deserialize;
use serde_json::{json, Value};
use tokio::sync::OnceCell;

use animus_log_storage_s3::S3LogBackend;

#[derive(Deserialize)]
struct StoreParams {
    #[serde(default)]
    entries: Vec<LogEntry>,
}

/// Lazily-initialized S3 backend. The backend reads its config (S3_BUCKET +
/// credentials) from the environment, but that env is only present at RUNTIME —
/// NOT during the install-time `--manifest` capability probe. Building the
/// backend eagerly in `main` made the probe fail ("S3_BUCKET unset"), so the
/// plugin could not be installed. Initializing on first real op keeps
/// `--manifest`/`initialize`/`health` working with no S3 env, and surfaces a
/// genuine misconfiguration only when a store/query is actually attempted.
struct LazyBackend {
    cell: OnceCell<S3LogBackend>,
}

impl LazyBackend {
    fn new() -> Self {
        Self {
            cell: OnceCell::new(),
        }
    }

    async fn get(&self) -> Result<&S3LogBackend, RpcError> {
        self.cell
            .get_or_try_init(|| async { S3LogBackend::from_env().await })
            .await
            .map_err(|e| RpcError {
                code: error_codes::INTERNAL_ERROR,
                message: format!("S3 log backend init failed: {e}"),
                data: None,
            })
    }
}

fn env_req(name: &str, description: &str, sensitive: bool, required: bool) -> EnvRequirement {
    EnvRequirement {
        name: name.to_string(),
        description: Some(description.to_string()),
        sensitive,
        required,
    }
}

fn rpc_from(err: animus_log_storage_protocol::BackendError) -> RpcError {
    err.into()
}

#[tokio::main(flavor = "multi_thread")]
async fn main() -> anyhow::Result<()> {
    // Logs go to stderr; stdout is the JSON-RPC channel. Never log secrets.
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let backend = Arc::new(LazyBackend::new());

    let store_backend = backend.clone();
    let query_backend = backend.clone();
    let tail_backend = backend.clone();
    let schema_backend = backend.clone();
    let health_backend = backend.clone();

    let plugin = Plugin::new(
        "animus-log-storage-s3",
        env!("CARGO_PKG_VERSION"),
        PLUGIN_KIND_LOG_STORAGE_BACKEND,
    )
    .description("S3-compatible log storage backend for Animus run logs")
    .streaming(true)
    .cancellation(true)
    .methods([
        METHOD_LOG_STORAGE_STORE,
        METHOD_LOG_STORAGE_QUERY,
        METHOD_LOG_STORAGE_TAIL,
        METHOD_LOG_STORAGE_SCHEMA,
        "health/check",
    ])
    .env_required([
        env_req("S3_BUCKET", "Target S3 bucket name", false, true),
        env_req("S3_ACCESS_KEY_ID", "S3 access key id", true, true),
        env_req("S3_SECRET_ACCESS_KEY", "S3 secret access key", true, true),
        env_req(
            "S3_ENDPOINT",
            "Custom S3 endpoint URL (Railway bucket / MinIO / R2). Omit for AWS S3.",
            false,
            false,
        ),
        env_req("S3_REGION", "S3 region (default: us-east-1)", false, false),
        env_req(
            "S3_PREFIX",
            "Key prefix applied to every object (default: bucket root)",
            false,
            false,
        ),
        env_req(
            "S3_FORCE_PATH_STYLE",
            "Path-style addressing endpoint/bucket/key (default: true)",
            false,
            false,
        ),
    ])
    .register_method::<StoreParams, Value, _, _>(METHOD_LOG_STORAGE_STORE, move |params, _ctx| {
        let backend = store_backend.clone();
        async move {
            backend
                .get()
                .await?
                .store(params.entries)
                .await
                .map_err(rpc_from)?;
            Ok(json!({}))
        }
    })
    .register_method::<LogQuery, _, _, _>(METHOD_LOG_STORAGE_QUERY, move |filter, _ctx| {
        let backend = query_backend.clone();
        async move { backend.get().await?.query(filter).await.map_err(rpc_from) }
    })
    .register_raw_method(
        METHOD_LOG_STORAGE_TAIL,
        move |params, ctx: MethodContext| {
            let backend = tail_backend.clone();
            async move {
                let filter: LogQuery = serde_json::from_value(params).map_err(|e| RpcError {
                    code: error_codes::INVALID_PARAMS,
                    message: format!("invalid {METHOD_LOG_STORAGE_TAIL} params: {e}"),
                    data: None,
                })?;
                // Snapshot the matching window, then stream it as
                // `log_storage/event` notifications carrying the request id.
                let result = backend.get().await?.query(filter).await.map_err(rpc_from)?;
                let request_id = ctx.request_id.clone();
                let notifier = ctx.notifier.clone();
                tokio::spawn(async move {
                    for entry in result.entries {
                        notifier
                            .notify(
                                NOTIFICATION_LOG_STORAGE_EVENT,
                                json!({ "id": request_id, "entry": entry }),
                            )
                            .await;
                    }
                    notifier
                        .notify(
                            NOTIFICATION_LOG_STORAGE_EVENT,
                            json!({ "id": request_id, "done": true }),
                        )
                        .await;
                });
                // Ack immediately; the stream arrives as notifications.
                Ok(json!({}))
            }
        },
    )
    .register_method::<Value, _, _, _>(METHOD_LOG_STORAGE_SCHEMA, move |_params: Value, _ctx| {
        let backend = schema_backend.clone();
        async move { Ok(backend.get().await?.schema()) }
    })
    .on_health(move || {
        let backend = health_backend.clone();
        async move { backend.get().await?.health().await.map_err(rpc_from) }
    });

    plugin.run().await
}
