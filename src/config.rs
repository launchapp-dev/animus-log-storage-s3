//! S3 backend configuration sourced entirely from the process environment.
//!
//! The daemon spawns the plugin with the operator's S3 credentials in the
//! environment (injected from the OS keychain / Animus secrets). Nothing is
//! read from disk and no secret is ever logged.

use std::env;

use anyhow::{anyhow, Result};

/// Resolved S3 connection settings.
#[derive(Clone)]
pub struct S3Config {
    /// Custom S3 endpoint URL (e.g. a Railway bucket endpoint or MinIO).
    /// `None` => the AWS default endpoint for `region` is used.
    pub endpoint: Option<String>,
    /// Target bucket name.
    pub bucket: String,
    /// Access key id.
    pub access_key_id: String,
    /// Secret access key.
    pub secret_access_key: String,
    /// Region. Many S3-compatible providers ignore it but the SDK requires a
    /// value; defaults to `us-east-1`.
    pub region: String,
    /// Key prefix applied to every object. Trailing slash is normalized away.
    pub prefix: String,
    /// Force path-style addressing (`endpoint/bucket/key`). Defaults to `true`
    /// because most S3-compatible providers (MinIO, Railway, many R2 setups)
    /// do not support virtual-hosted-style addressing.
    pub force_path_style: bool,
}

impl std::fmt::Debug for S3Config {
    // Never print credentials.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("S3Config")
            .field("endpoint", &self.endpoint)
            .field("bucket", &self.bucket)
            .field("region", &self.region)
            .field("prefix", &self.prefix)
            .field("force_path_style", &self.force_path_style)
            .field("access_key_id", &"<redacted>")
            .field("secret_access_key", &"<redacted>")
            .finish()
    }
}

fn env_opt(key: &str) -> Option<String> {
    match env::var(key) {
        Ok(value) if !value.trim().is_empty() => Some(value),
        _ => None,
    }
}

fn env_required(key: &str) -> Result<String> {
    env_opt(key).ok_or_else(|| anyhow!("required environment variable {key} is unset or empty"))
}

/// Normalize a user-supplied prefix into a slash-free-suffixed segment with no
/// leading or trailing slashes (empty string means "bucket root").
fn normalize_prefix(raw: &str) -> String {
    raw.trim().trim_matches('/').to_string()
}

impl S3Config {
    /// Build the configuration from the environment.
    ///
    /// Reads `S3_BUCKET`, `S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`
    /// (required), and `S3_ENDPOINT`, `S3_REGION`, `S3_PREFIX`,
    /// `S3_FORCE_PATH_STYLE` (optional).
    pub fn from_env() -> Result<Self> {
        let bucket = env_required("S3_BUCKET")?;
        let access_key_id = env_required("S3_ACCESS_KEY_ID")?;
        let secret_access_key = env_required("S3_SECRET_ACCESS_KEY")?;
        let endpoint = env_opt("S3_ENDPOINT");
        let region = env_opt("S3_REGION").unwrap_or_else(|| "us-east-1".to_string());
        let prefix = normalize_prefix(&env_opt("S3_PREFIX").unwrap_or_default());
        let force_path_style = env_opt("S3_FORCE_PATH_STYLE")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(true);

        Ok(Self {
            endpoint,
            bucket,
            access_key_id,
            secret_access_key,
            region,
            prefix,
            force_path_style,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalize_prefix_strips_slashes() {
        assert_eq!(normalize_prefix("/animus-logs/"), "animus-logs");
        assert_eq!(normalize_prefix("animus/logs"), "animus/logs");
        assert_eq!(normalize_prefix("   "), "");
        assert_eq!(normalize_prefix(""), "");
    }

    #[test]
    fn debug_redacts_credentials() {
        let cfg = S3Config {
            endpoint: Some("https://example.com".into()),
            bucket: "logs".into(),
            access_key_id: "AKIASECRET".into(),
            secret_access_key: "supersecret".into(),
            region: "us-east-1".into(),
            prefix: "p".into(),
            force_path_style: true,
        };
        let rendered = format!("{cfg:?}");
        assert!(!rendered.contains("AKIASECRET"));
        assert!(!rendered.contains("supersecret"));
        assert!(rendered.contains("<redacted>"));
    }
}
