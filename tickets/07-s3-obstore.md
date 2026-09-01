# 07 — S3/object storage via obstore `url:`

Status: done

Built as sketched; measured notes (2026-09-01, obstore 0.11.1, versitygw 1.7.0
as the live S3 system instead of MinIO/Tigris):

- `from_url("s3://bucket/prefix")` bakes the URL path in as the store prefix,
  so the exporter needed zero changes — the whole point of the obstore seam.
- Credentials work exactly as designed: `AWS_ACCESS_KEY_ID` /
  `AWS_SECRET_ACCESS_KEY` / `AWS_ENDPOINT_URL` / `AWS_ALLOW_HTTP=true` env
  vars, nothing in plugin config. **Do not pass `client_options` to
  `from_url`** — it replaces the env-derived client config wholesale (found
  out when it clobbered `AWS_ALLOW_HTTP` → BadScheme against the http
  gateway).
- Retry: obstore's built-in retry defaults are generous (10 retries); the
  plugin passes `retry_config` with `max_retries: 1`, 30s retry budget,
  250ms→2s backoff. Measured: a connection-refused endpoint fails a PUT in
  0.25s; a hung endpoint is bounded by obstore's ~30s per-request timeout.
  The exporter drops the failed batch with one stderr line per error class
  (message-level dedupe logged every batch — messages embed per-file keys).
- Shutdown: the SDK's `BatchProcessor.shutdown(timeout_millis=30000)` caps
  the worker join, then calls our `shutdown()` unbounded — the retry_config
  above is what actually bounds exit. Verified live: SIGINT on a server
  writing to versitygw flushed the tail file to the bucket and exited
  promptly.
- Live acceptance ran via `just s3-gateway` + `just demo-s3` + `just
  query-s3` — including an unplanned outage test (server up before the
  gateway: batches dropped with a log line, clean recovery once the gateway
  came up). versitygw gotcha: its posix backend root must be an absolute
  path (it resolves after a chdir).

Because ticket 03 writes bytes through an obstore store, this ticket is config
plumbing and docs, not exporter changes. The whole point of choosing obstore early.

## Design sketch

```yaml
plugins:
  datasette-otel-parquet:
    url: s3://my-bucket/telemetry     # any obstore-supported URL: s3://, gs://, az://
    # region/endpoint/credentials: NOT plugin config — see below
```

- `url` and `path` are mutually exclusive; both set → startup error (loud, not
  last-one-wins).
- Store construction: `obstore.store.from_url(url)` and let obstore's native
  credential chain do the rest — standard `AWS_*` env vars (including
  `AWS_ENDPOINT_URL` for R2/MinIO/Tigris), instance metadata, automatic refresh
  before expiry. **No credentials in plugin config, ever** — datasette.yaml gets
  committed to repos. If someone needs a non-env knob later, the answer is
  datasette-secrets integration, a separate conversation.
- Extra store options (`region`, `virtual_hosted_style`...) — start with none; add an
  opaque `store_options: {}` dict passed through to `from_url(**...)` only when a real
  user hits a wall. Every option we mirror is API surface to maintain.
- Path layout unchanged: keys are `telemetry-prefix + traces/<yyyy>/.../<id>.parquet`,
  where the prefix comes from the URL path.

## What changes operationally (document, don't engineer around)

- Each flush is now a network PUT from the exporter thread. Fine at default cadence
  (one small PUT per ~10s); document that `flush_interval_seconds: 1` against S3 is a
  cost/latency decision the operator is making.
- A PUT can fail transiently. v1 policy: one retry (obstore has built-in retry
  behavior — check what it does before adding our own), then drop the batch with one
  log line. Spans are telemetry, not ledger entries; never buffer unboundedly toward
  an unreachable bucket.
- Shutdown flush now blocks exit on a network call — cap it (`shutdown` already takes
  a timeout through the SDK; verify the path).
- The DuckDB cookbook gains the remote version:
  `read_parquet('s3://my-bucket/telemetry/traces/**/*.parquet')` with
  `CREATE SECRET` / env creds — the celld story verbatim, "a Datasette with a bucket
  has observability with no other service".

## Acceptance

- Against MinIO (or Tigris) in a Justfile recipe: browse → files appear in the
  bucket → DuckDB queries them remotely.
- Unit tests fake the store (same seam as ticket 04's write-failure test); no network
  in CI.
- README documents env-var credentials for AWS + one S3-compatible (R2 or Tigris).
