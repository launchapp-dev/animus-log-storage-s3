# animus-log-storage-s3

An Animus `log_storage_backend` plugin that persists agent run logs to an
**S3-compatible bucket** — Railway buckets, MinIO, Cloudflare R2, or AWS S3.
This makes run logs durable and offloads them from the ephemeral container
disk, so logs survive container restarts and redeploys.

It implements the `animus-log-storage-protocol` role (`log_storage/store`,
`log_storage/query`, `log_storage/tail`, `log_storage/schema`) and needs no
kernel changes — the role already exists in Animus.

## How it works

Each log entry is stored as a single JSON object. The object key encodes the
queryable dimensions as prefix segments so that querying by source — and by a
recent time window — is a cheap `ListObjectsV2` prefix scan plus targeted
`GetObject`s:

```
<S3_PREFIX>/<source>/<source_name>/<YYYY>/<MM>/<DD>/<ts_millis>-<id>.json
```

- `<source>` — `daemon` | `plugin` | `cli` | `workflow`. Server-side
  `by_source` filtering is exactly prefix narrowing.
- `<source_name>` — the emitter name (plugin name, workflow id, CLI command),
  sanitized to `[A-Za-z0-9._-]`; `_` when absent.
- `<YYYY>/<MM>/<DD>` — UTC date of the entry, for time-window scans.
- `<ts_millis>` — zero-padded epoch milliseconds, so lexical key order equals
  chronological order within a day. "Most recent" is a sorted slice.
- `<id>` — the entry's dedup id. The same `(ts, id)` derives the same key, so
  a re-store overwrites in place — at-least-once writes are idempotent.

### Query filters

The backend declares its capabilities honestly via `log_storage/schema`:

| `LogStorageSchema` field         | Value   | Notes |
|----------------------------------|---------|-------|
| `supports_query`                 | `true`  | prefix list + targeted GET |
| `supports_tail`                  | `true`  | best-effort one-shot snapshot (no live follow — object storage has no change feed) |
| `supports_dedup`                 | `true`  | idempotent by derived key |
| `supports_filtering.by_source`   | `true`  | server-side prefix narrowing |
| `supports_filtering.by_level`    | `true`  | applied after fetch |
| `supports_filtering.by_time_range` | `true` | date-prefix scoping + key pre-filter + applied after fetch |
| `supports_filtering.by_target`   | `true`  | glob applied after fetch |
| `supports_filtering.by_glob`     | `true`  | full `*` / `**` glob semantics |
| `max_query_window`               | `None`  | bounded by retention only |
| `retention_hint`                 | `None`  | retention is an S3 lifecycle policy you set on the bucket |

`source` and `source_name` narrow the list prefix server-side. When both are
pinned and the query carries a `since` floor, the scan addresses the
`<YYYY>/<MM>/<DD>` date segments directly — enumerating one concrete day prefix
per UTC day in the window (newest first, up to a bounded span) instead of
listing the whole source history. Object bodies are then fetched concurrently,
newest window first, and the scan stops as soon as `limit` matches are found —
so a small `limit` never downloads the whole bucket. The remaining filters
(level floor, exact `source_name`, target glob, precise time bounds) are
evaluated in-process after fetching object bodies. `since > until` returns a
domain `INVALID_PARAMS` error. A query with no `limit` returns at most 500
entries (most-recent first by key, then sorted oldest-first per the protocol).

`tail` returns the current matching window as a finite stream of
`log_storage/event` notifications (then `{ done: true }`); `follow = true`
cannot be honored over object storage, so it still returns the snapshot rather
than erroring. The daemon's historical `daemon logs` path uses `query`.

## S3 client

Uses the official **`aws-sdk-s3`** + **`aws-config`**. Chosen over `rust-s3`
because it is the most robust path to any S3-compatible endpoint: static
credentials (no IMDS / profile resolution), `endpoint_url` for custom
endpoints, and `force_path_style` (default `true`) for MinIO / Railway / R2
which generally do not support virtual-hosted-style addressing. It also gets
first-class retries, checksums, and continuation-token paging for free.

## Environment variables

Secrets come from the Animus secret store / the daemon process environment.
Nothing is read from disk and no secret is ever logged.

| Variable               | Required | Default     | Description |
|------------------------|----------|-------------|-------------|
| `S3_BUCKET`            | yes      | —           | Target bucket name |
| `S3_ACCESS_KEY_ID`     | yes      | —           | Access key id |
| `S3_SECRET_ACCESS_KEY` | yes      | —           | Secret access key |
| `S3_ENDPOINT`          | no       | AWS default | Custom endpoint URL (Railway bucket / MinIO / R2). Omit for AWS S3. |
| `S3_REGION`            | no       | `us-east-1` | Region (many S3-compatible providers ignore it) |
| `S3_PREFIX`            | no       | bucket root | Key prefix applied to every object |
| `S3_FORCE_PATH_STYLE`  | no       | `true`      | Path-style addressing; set `false` only for providers that require virtual-hosted-style |

## Install

```bash
animus plugin install launchapp-dev/animus-log-storage-s3
```

Set the S3 credentials as Animus secrets for the project's repo-scope:

```bash
animus secret set S3_ACCESS_KEY_ID
animus secret set S3_SECRET_ACCESS_KEY
```

and the non-secret config in the daemon environment (or also as secrets):

```bash
export S3_ENDPOINT="https://<bucket>.<region>.railwayobjectstorage.com"   # Railway bucket endpoint
export S3_BUCKET="animus-logs"
export S3_REGION="us-east-1"
export S3_PREFIX="animus"
```

Then restart the daemon. With a `log_storage_backend` plugin installed, the
daemon routes log entries through `log_storage/store` and `daemon logs` reads
back through `log_storage/query`.

## Railway bucket setup

1. In your Railway project, add a **Bucket** (S3-compatible object storage).
2. From the bucket's **Connect** / variables, collect: the **endpoint URL**,
   **bucket name**, **access key id**, and **secret access key** (Railway
   exposes these as connection variables).
3. Set them on the Animus service:
   - `S3_ENDPOINT` = the bucket endpoint URL
   - `S3_BUCKET` = the bucket name
   - `S3_ACCESS_KEY_ID` / `S3_SECRET_ACCESS_KEY` = the bucket credentials
     (store via `animus secret set`)
   - `S3_REGION` = `us-east-1` (or as provided; usually ignored)
   - `S3_FORCE_PATH_STYLE` = `true` (default; leave as-is)
4. Restart the Animus daemon. Verify with `animus daemon health` (the plugin
   reports `Unhealthy` with the bucket error string if it cannot reach the
   bucket) and `animus logs` / `daemon logs`.

## License

Elastic License 2.0.
