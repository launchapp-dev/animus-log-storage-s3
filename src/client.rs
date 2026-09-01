//! Thin async wrapper over `aws-sdk-s3` configured for any S3-compatible
//! endpoint (Railway bucket, MinIO, Cloudflare R2, AWS S3).

use aws_config::{BehaviorVersion, Region};
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::Client;

use crate::config::S3Config;

/// Minimal object-store surface the backend logic needs: put/get one object
/// body and list keys under a prefix. Abstracted from `S3Client` so the
/// store/query/tail logic is testable without a live bucket.
#[async_trait::async_trait]
pub trait ObjectStore: Send + Sync {
    /// Put a single object (idempotent — same key overwrites).
    async fn put_object(&self, key: &str, body: Vec<u8>) -> anyhow::Result<()>;
    /// Get a single object body as bytes.
    async fn get_object(&self, key: &str) -> anyhow::Result<Vec<u8>>;
    /// List every key under `prefix` (bodies are fetched separately).
    async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>>;
    /// Cheap reachability probe for health reporting. The default lists keys
    /// at the bucket root, which may be unbounded — implementations backed by
    /// a real object store should override this with a bounded probe.
    async fn ping(&self) -> Result<(), String> {
        self.list_keys("")
            .await
            .map(|_| ())
            .map_err(|err| err.to_string())
    }
}

/// Built S3 client bound to a single bucket.
#[derive(Clone)]
pub struct S3Client {
    client: Client,
    bucket: String,
}

impl S3Client {
    /// Construct the client from resolved [`S3Config`]. Does not perform any
    /// network I/O — credentials are static and the endpoint is applied
    /// directly, so no IMDS / profile resolution happens.
    pub async fn connect(cfg: &S3Config) -> Self {
        let credentials = Credentials::new(
            cfg.access_key_id.clone(),
            cfg.secret_access_key.clone(),
            None,
            None,
            "animus-log-storage-s3",
        );

        let mut builder = S3ConfigBuilder::new()
            .behavior_version(BehaviorVersion::latest())
            .region(Region::new(cfg.region.clone()))
            .credentials_provider(credentials)
            .force_path_style(cfg.force_path_style);

        if let Some(endpoint) = &cfg.endpoint {
            builder = builder.endpoint_url(endpoint.clone());
        }

        let client = Client::from_conf(builder.build());
        Self {
            client,
            bucket: cfg.bucket.clone(),
        }
    }

    /// Bucket this client targets.
    pub fn bucket(&self) -> &str {
        &self.bucket
    }

    /// Put a single object (idempotent — same key overwrites).
    pub async fn put_object(&self, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(key)
            .body(ByteStream::from(body))
            .content_type("application/json")
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("s3 put_object {key}: {}", display_sdk_err(e)))?;
        Ok(())
    }

    /// Get a single object body as bytes.
    pub async fn get_object(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        let resp = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("s3 get_object {key}: {}", display_sdk_err(e)))?;
        let data = resp
            .body
            .collect()
            .await
            .map_err(|e| anyhow::anyhow!("s3 get_object {key} body: {e}"))?;
        Ok(data.into_bytes().to_vec())
    }

    /// List every key under `prefix`, walking continuation tokens. Returns the
    /// keys (not bodies); the caller fetches bodies it actually wants.
    pub async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        let mut keys = Vec::new();
        let mut continuation: Option<String> = None;
        loop {
            let mut req = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(prefix);
            if let Some(token) = &continuation {
                req = req.continuation_token(token);
            }
            let resp = req.send().await.map_err(|e| {
                anyhow::anyhow!("s3 list_objects_v2 {prefix}: {}", display_sdk_err(e))
            })?;
            for obj in resp.contents() {
                if let Some(key) = obj.key() {
                    keys.push(key.to_string());
                }
            }
            if resp.is_truncated().unwrap_or(false) {
                continuation = resp.next_continuation_token().map(|s| s.to_string());
                if continuation.is_none() {
                    break;
                }
            } else {
                break;
            }
        }
        Ok(keys)
    }

    /// Cheap reachability probe for health: a single bounded list against the
    /// bucket root. Returns the error string on failure.
    pub async fn ping(&self) -> Result<(), String> {
        self.client
            .list_objects_v2()
            .bucket(&self.bucket)
            .max_keys(1)
            .send()
            .await
            .map(|_| ())
            .map_err(display_sdk_err)
    }
}

#[async_trait::async_trait]
impl ObjectStore for S3Client {
    async fn put_object(&self, key: &str, body: Vec<u8>) -> anyhow::Result<()> {
        S3Client::put_object(self, key, body).await
    }

    async fn get_object(&self, key: &str) -> anyhow::Result<Vec<u8>> {
        S3Client::get_object(self, key).await
    }

    async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
        S3Client::list_keys(self, prefix).await
    }

    async fn ping(&self) -> Result<(), String> {
        S3Client::ping(self).await
    }
}

/// Render an SDK error without leaking request internals; surfaces the service
/// message when present.
fn display_sdk_err<E, R>(err: aws_sdk_s3::error::SdkError<E, R>) -> String
where
    E: std::error::Error,
{
    use aws_sdk_s3::error::SdkError;
    match err {
        SdkError::ServiceError(ctx) => format!("service error: {}", ctx.err()),
        other => other.to_string(),
    }
}
