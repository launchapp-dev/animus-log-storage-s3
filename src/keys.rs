//! Object-key derivation and in-process query filtering.
//!
//! # Object layout
//!
//! Each [`LogEntry`] is stored as one JSON object. The key is derived so that
//! the queryable top-level dimensions become prefix segments, letting
//! `query` translate a filter into a cheap S3 `ListObjectsV2` prefix scan:
//!
//! ```text
//! <prefix>/<source>/<source_name>/<YYYY>/<MM>/<DD>/<ts_millis>-<id>.json
//! ```
//!
//! - `<source>` — `daemon` | `plugin` | `cli` | `workflow` (server-side
//!   `by_source` filter == prefix narrowing).
//! - `<source_name>` — sanitized [`LogEntry::source_name`], or `_` when the
//!   emitter set none (server-side `by_source_name` filter, though the
//!   protocol's `SupportsFiltering` does not expose a `by_source_name` flag,
//!   so we keep that one in-process).
//! - `<YYYY>/<MM>/<DD>` — UTC date of the entry; lets a time-range query
//!   bound the listed prefixes (`by_time_range`).
//! - `<ts_millis>` — zero-padded epoch-millis so lexical key order == time
//!   order within a day, making "most recent" a list-tail / sorted slice.
//! - `<id>` — the backend dedup id; identical `(ts, id)` overwrites the same
//!   object, giving idempotent at-least-once `store` semantics.

use animus_log_storage_protocol::{LogEntry, LogLevel, LogQuery, LogSource};
use chrono::{DateTime, Datelike, Utc};

/// Epoch-millis are zero-padded to 14 digits so lexical order matches
/// chronological order for any timestamp up to the year ~5138.
const TS_WIDTH: usize = 14;

fn source_segment(source: LogSource) -> &'static str {
    match source {
        LogSource::Daemon => "daemon",
        LogSource::Plugin => "plugin",
        LogSource::Cli => "cli",
        LogSource::Workflow => "workflow",
    }
}

/// Sanitize an arbitrary string into a single safe S3 key segment: keep
/// `[A-Za-z0-9._-]`, replace everything else (including `/`) with `_`. Empty
/// input collapses to `_`.
fn sanitize_segment(raw: &str) -> String {
    let mut out = String::with_capacity(raw.len());
    for ch in raw.chars() {
        if ch.is_ascii_alphanumeric() || matches!(ch, '.' | '_' | '-') {
            out.push(ch);
        } else {
            out.push('_');
        }
    }
    if out.is_empty() {
        out.push('_');
    }
    out
}

/// Join a normalized (slash-free-suffixed) prefix to a key body.
fn with_prefix(prefix: &str, body: &str) -> String {
    if prefix.is_empty() {
        body.to_string()
    } else {
        format!("{prefix}/{body}")
    }
}

/// Derive the full object key for an entry under `prefix`.
pub fn entry_key(prefix: &str, entry: &LogEntry) -> String {
    let source = source_segment(entry.source);
    let name = sanitize_segment(entry.source_name.as_deref().unwrap_or("_"));
    let date = entry.ts.date_naive();
    let id = sanitize_segment(&entry.id);
    let millis = entry.ts.timestamp_millis().max(0) as u64;
    let body = format!(
        "{source}/{name}/{y:04}/{m:02}/{d:02}/{millis:0width$}-{id}.json",
        y = date.year(),
        m = date.month(),
        d = date.day(),
        width = TS_WIDTH,
    );
    with_prefix(prefix, &body)
}

/// The narrowest static list-prefix that still covers every key matching
/// `filter`. Used to bound the S3 `ListObjectsV2` scan; the remaining filter
/// predicates are evaluated in-process by [`matches`].
///
/// Only `source` narrows the prefix today (it is the first key segment).
/// `since`/`until` could narrow on the date segments but only when both the
/// source and source_name are pinned; we keep the prefix conservative
/// (source-only) so a time-range query without a source still works.
pub fn list_prefix(prefix: &str, filter: &LogQuery) -> String {
    match filter.source {
        Some(source) => with_prefix(prefix, &format!("{}/", source_segment(source))),
        None => {
            if prefix.is_empty() {
                String::new()
            } else {
                format!("{prefix}/")
            }
        }
    }
}

/// Evaluate the in-process predicates of `filter` against a decoded entry.
///
/// The S3 prefix scan already applied the `source` narrowing; this catches
/// everything the prefix cannot express: level floor, exact source_name,
/// target glob, and the time-range bounds.
pub fn matches(entry: &LogEntry, filter: &LogQuery) -> bool {
    if let Some(min) = filter.min_level {
        if !level_at_least(entry.level, min) {
            return false;
        }
    }
    if let Some(source) = filter.source {
        if entry.source != source {
            return false;
        }
    }
    if let Some(name) = &filter.source_name {
        if entry.source_name.as_deref() != Some(name.as_str()) {
            return false;
        }
    }
    if let Some(glob) = &filter.target_glob {
        if !target_glob_matches(glob, &entry.target) {
            return false;
        }
    }
    if let Some(since) = filter.since {
        if entry.ts < since {
            return false;
        }
    }
    if let Some(until) = filter.until {
        if entry.ts >= until {
            return false;
        }
    }
    true
}

/// Order entries chronologically (oldest first), as `LogQueryResult` requires.
pub fn sort_chronological(entries: &mut [LogEntry]) {
    entries.sort_by(|a, b| a.ts.cmp(&b.ts).then_with(|| a.id.cmp(&b.id)));
}

fn level_rank(level: LogLevel) -> u8 {
    match level {
        LogLevel::Trace => 0,
        LogLevel::Debug => 1,
        LogLevel::Info => 2,
        LogLevel::Warn => 3,
        LogLevel::Error => 4,
    }
}

fn level_at_least(level: LogLevel, floor: LogLevel) -> bool {
    level_rank(level) >= level_rank(floor)
}

/// Glob match per the protocol convention:
/// `*` matches any run of non-`.` characters, `**` matches any run including
/// `.`. Implemented as a small backtracking matcher over `.`-segmented input
/// where `*` is segment-local and `**` spans segments.
fn target_glob_matches(pattern: &str, target: &str) -> bool {
    glob_rec(pattern.as_bytes(), target.as_bytes())
}

fn glob_rec(pat: &[u8], txt: &[u8]) -> bool {
    if pat.is_empty() {
        return txt.is_empty();
    }
    match pat[0] {
        b'*' => {
            if pat.len() >= 2 && pat[1] == b'*' {
                // `**` — consume any run of characters including `.`.
                let rest = &pat[2..];
                // Try consuming 0..=txt.len() chars.
                for i in 0..=txt.len() {
                    if glob_rec(rest, &txt[i..]) {
                        return true;
                    }
                }
                false
            } else {
                // `*` — consume any run of non-`.` characters.
                let rest = &pat[1..];
                let mut i = 0;
                loop {
                    if glob_rec(rest, &txt[i..]) {
                        return true;
                    }
                    if i >= txt.len() || txt[i] == b'.' {
                        return false;
                    }
                    i += 1;
                }
            }
        }
        c => {
            if txt.is_empty() || txt[0] != c {
                false
            } else {
                glob_rec(&pat[1..], &txt[1..])
            }
        }
    }
}

/// Parse the UTC timestamp embedded in a derived key's filename, used to skip
/// fetching objects outside a time window during the list scan. Returns
/// `None` if the key does not look like one of ours.
pub fn ts_millis_from_key(key: &str) -> Option<i64> {
    let file = key.rsplit('/').next()?;
    let dash = file.find('-')?;
    file[..dash].parse::<i64>().ok()
}

/// Cheap pre-filter on the key alone (before fetching the object body): drop
/// keys whose embedded millis fall entirely outside the `[since, until)`
/// window. Conservative — keeps anything it cannot parse.
pub fn key_in_time_window(
    key: &str,
    since: Option<DateTime<Utc>>,
    until: Option<DateTime<Utc>>,
) -> bool {
    let Some(millis) = ts_millis_from_key(key) else {
        return true;
    };
    if let Some(s) = since {
        if millis < s.timestamp_millis() {
            return false;
        }
    }
    if let Some(u) = until {
        if millis >= u.timestamp_millis() {
            return false;
        }
    }
    true
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;

    fn entry(id: &str, source: LogSource, name: Option<&str>, ts: DateTime<Utc>) -> LogEntry {
        LogEntry {
            id: id.into(),
            ts,
            level: LogLevel::Info,
            source,
            source_name: name.map(|s| s.into()),
            target: "daemon.scheduler.tick".into(),
            message: "m".into(),
            fields: serde_json::Value::Null,
        }
    }

    fn ts(s: &str) -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(s).unwrap().with_timezone(&Utc)
    }

    #[test]
    fn entry_key_layout_is_prefixed_and_time_sortable() {
        let e1 = entry(
            "a",
            LogSource::Workflow,
            Some("WF-1"),
            ts("2026-06-25T10:00:00Z"),
        );
        let e2 = entry(
            "b",
            LogSource::Workflow,
            Some("WF-1"),
            ts("2026-06-25T10:00:01Z"),
        );
        let k1 = entry_key("animus-logs", &e1);
        let k2 = entry_key("animus-logs", &e2);
        assert!(k1.starts_with("animus-logs/workflow/WF-1/2026/06/25/"));
        assert!(k1.ends_with("-a.json"));
        // Lexical order == chronological order within the same day.
        assert!(k1 < k2);
    }

    #[test]
    fn entry_key_empty_prefix_has_no_leading_slash() {
        let e = entry("a", LogSource::Daemon, None, ts("2026-06-25T10:00:00Z"));
        let k = entry_key("", &e);
        assert!(k.starts_with("daemon/_/2026/06/25/"));
        assert!(!k.starts_with('/'));
    }

    #[test]
    fn sanitize_segment_replaces_unsafe_chars() {
        assert_eq!(
            sanitize_segment("animus-subject/linear"),
            "animus-subject_linear"
        );
        assert_eq!(sanitize_segment("a b@c"), "a_b_c");
        assert_eq!(sanitize_segment(""), "_");
        assert_eq!(sanitize_segment("keep.dot_dash-1"), "keep.dot_dash-1");
    }

    #[test]
    fn list_prefix_narrows_on_source_only() {
        let q = LogQuery {
            source: Some(LogSource::Cli),
            ..Default::default()
        };
        assert_eq!(list_prefix("p", &q), "p/cli/");
        let q2 = LogQuery::default();
        assert_eq!(list_prefix("p", &q2), "p/");
        assert_eq!(list_prefix("", &q2), "");
    }

    #[test]
    fn level_floor_filters() {
        let mut e = entry("a", LogSource::Daemon, None, ts("2026-06-25T10:00:00Z"));
        e.level = LogLevel::Info;
        let q = LogQuery {
            min_level: Some(LogLevel::Warn),
            ..Default::default()
        };
        assert!(!matches(&e, &q));
        e.level = LogLevel::Error;
        assert!(matches(&e, &q));
    }

    #[test]
    fn source_name_exact_match() {
        let e = entry(
            "a",
            LogSource::Plugin,
            Some("animus-subject-linear"),
            ts("2026-06-25T10:00:00Z"),
        );
        let hit = LogQuery {
            source_name: Some("animus-subject-linear".into()),
            ..Default::default()
        };
        let miss = LogQuery {
            source_name: Some("other".into()),
            ..Default::default()
        };
        assert!(matches(&e, &hit));
        assert!(!matches(&e, &miss));
    }

    #[test]
    fn time_range_bounds_are_inclusive_since_exclusive_until() {
        let e = entry("a", LogSource::Daemon, None, ts("2026-06-25T10:00:00Z"));
        let inside = LogQuery {
            since: Some(ts("2026-06-25T10:00:00Z")),
            until: Some(ts("2026-06-25T10:00:01Z")),
            ..Default::default()
        };
        assert!(matches(&e, &inside));
        let at_until = LogQuery {
            until: Some(ts("2026-06-25T10:00:00Z")),
            ..Default::default()
        };
        assert!(!matches(&e, &at_until)); // until is exclusive
    }

    #[test]
    fn target_glob_semantics() {
        assert!(target_glob_matches(
            "daemon.scheduler.*",
            "daemon.scheduler.tick"
        ));
        assert!(!target_glob_matches(
            "daemon.scheduler.*",
            "daemon.scheduler.tick.inner"
        ));
        assert!(target_glob_matches(
            "daemon.**",
            "daemon.scheduler.tick.inner"
        ));
        assert!(target_glob_matches("**", "anything.at.all"));
        assert!(!target_glob_matches("plugin.*", "daemon.scheduler"));
    }

    #[test]
    fn key_time_window_prefilter() {
        let e = entry("a", LogSource::Daemon, None, ts("2026-06-25T10:00:00Z"));
        let key = entry_key("p", &e);
        let before = Utc.timestamp_opt(0, 0).unwrap();
        let after = ts("2030-01-01T00:00:00Z");
        assert!(!key_in_time_window(&key, Some(after), None));
        assert!(key_in_time_window(&key, Some(before), Some(after)));
        // Unparseable keys are conservatively kept.
        assert!(key_in_time_window("p/not-ours.txt", Some(after), None));
    }

    #[test]
    fn sort_chronological_orders_oldest_first() {
        let mut v = vec![
            entry("b", LogSource::Daemon, None, ts("2026-06-25T10:00:02Z")),
            entry("a", LogSource::Daemon, None, ts("2026-06-25T10:00:01Z")),
        ];
        sort_chronological(&mut v);
        assert_eq!(v[0].id, "a");
        assert_eq!(v[1].id, "b");
    }
}
