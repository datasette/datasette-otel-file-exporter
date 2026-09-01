# datasette-otel-parquet

Flush the OpenTelemetry spans Datasette emits into Parquet files — a local
directory, or any S3-compatible bucket. No collector, no tracing backend, no
extra service: your traces are just files you own, and anything that reads
Parquet queries them — DuckDB, pandas, Datasette itself. For a
data-exploration tool the payoff is circular in the best way: the traces of
your Datasette become a dataset. The file layout is borrowed from
[celld.dev's telemetry design](https://celld.dev/docs/telemetry/), which
proved the "a fleet with a bucket has observability with no other service"
idea this plugin brings to Datasette.

**Status: pre-release.** Requires the Datasette 1.0 alpha with OpenTelemetry
support (simonw/datasette PRs #2862–#2864) — no released Datasette emits
these spans yet.

## Quickstart

```bash
datasette install datasette-otel-parquet
datasette mydb.db -s plugins.datasette-otel-parquet.path ./telemetry
```

Browse a few pages, then (from another terminal — files are immutable once
written, so there is no lock contention with the live server):

```bash
duckdb -c "SELECT name, round(duration_ns / 1e6, 2) AS ms
           FROM read_parquet('telemetry/traces/**/*.parquet')
           ORDER BY ms DESC LIMIT 5;"
```

```
┌──────────────────────────────────────────────────────────────────────┬────────┐
│                                 name                                 │   ms   │
├──────────────────────────────────────────────────────────────────────┼────────┤
│ GET /(?P<database>[^\/\.]+)/(?P<table>[^\/\.]+)(\.(?P<format>\w+))?$ │  30.94 │
│ datasette.startup                                                    │   7.70 │
│ db.write.execute                                                     │   4.59 │
│ db.query                                                             │   1.33 │
│ db.query                                                             │   1.31 │
└──────────────────────────────────────────────────────────────────────┴────────┘
```

That is the entire setup. Every request trace, every SQL statement Datasette
ran, with timings, in files on your disk.

## ⚠️ Privacy

**These files contain SQL text.** `db.query.text` is recorded (truncated by
Datasette core), and on a public Datasette instance that SQL is
**user-supplied** — anyone hitting `?sql=` writes into your telemetry. SQL
**parameter *values* are never recorded** — only a parameter count — but
table names, column names and query shapes are all there. With `url:`
configured, shipping all of that off-box is one config line. Treat the
telemetry directory/bucket with the same care as the database it describes.

## Configuration

```yaml
# datasette.yaml
plugins:
  datasette-otel-parquet:
    path: ./telemetry              # local directory (created if absent)
    # url: s3://my-bucket/telemetry  # ...or any obstore URL: s3:// gs:// az://
    flush_interval_seconds: 10     # roll a new file at most this often
    max_buffer_spans: 10000        # ...or when this many spans are buffered
    service_name: my-datasette     # default: "datasette"
```

- `path` **or** `url`, not both (configuring both is a startup error).
  Neither configured → the plugin stays dormant: one stderr line, no files,
  no recording overhead.
- `service_name` sets the `service.name` resource attribute (ignored when
  `OTEL_SERVICE_NAME` is set, or when another provider owns tracing — see
  Coexistence).
- Spans also flush on shutdown: Ctrl-C does not lose the tail.
- Explicit `OTEL_*` environment variables always beat plugin config.

### Object storage credentials

Credentials are **never** plugin config — `datasette.yaml` gets committed to
repos. `url:` stores authenticate through obstore's native chain: standard
environment variables, instance metadata / IAM roles, with automatic refresh.

AWS S3:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
datasette mydb.db -s plugins.datasette-otel-parquet.url s3://my-bucket/telemetry
```

Any S3-compatible service (Cloudflare R2, Tigris, MinIO, versitygw) is the
same plus an endpoint:

```bash
export AWS_ENDPOINT_URL=https://fly.storage.tigris.dev   # or your R2/MinIO URL
export AWS_ALLOW_HTTP=true                               # only for http:// endpoints
```

Operational notes for buckets: each flush is one network PUT from a
background thread (never the event loop). `flush_interval_seconds: 1`
against S3 is a cost/latency decision you are making. A failed PUT is
retried once, then the batch is dropped with one stderr line — spans are
telemetry, not ledger entries; the plugin never buffers unboundedly toward
an unreachable bucket, and never blocks process exit for long on one.

## File layout

```
<path or url prefix>/traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.parquet
```

Hour partitions (UTC, from the flush wall clock); `file_id` is
`<unix_millis>-<random>` so names sort chronologically. Files are written
once and never touched again. Small files are expected — DuckDB globs cope;
compaction is deliberately out of scope.

## Schema

One row per span, flat, no nesting. The open-ended parts are JSON strings —
`attributes ->> 'db.query.text'` works directly in DuckDB.

| column | arrow type | notes |
|---|---|---|
| `trace_id` | string | 32 lowercase hex chars (OTLP/JSON style, human-pasteable) |
| `span_id` | string | 16 hex chars |
| `parent_span_id` | string, nullable | null for root spans |
| `name` | string | |
| `kind` | string | `SERVER` / `INTERNAL` / `CLIENT` / ... (SpanKind name, not int) |
| `start_time` | timestamp[us, UTC] | convenience column; DuckDB time-buckets it directly |
| `start_time_unix_nano` | int64 | full precision |
| `end_time_unix_nano` | int64 | |
| `duration_ns` | int64 | derived, but the column every query wants |
| `status_code` | string | `UNSET` / `OK` / `ERROR` |
| `status_message` | string, nullable | |
| `service_name` | string | pulled out of resource — the facetable column |
| `scope_name` | string | instrumentation scope (`datasette`) |
| `attributes` | string | JSON object; `attributes ->> 'db.query.text'` in DuckDB |
| `resource` | string | JSON object, full resource attrs (includes service.name again) |
| `events` | string, nullable | JSON array, null when empty |
| `links` | string, nullable | JSON array, null when empty |

Every file embeds `datasette_otel_parquet_schema = "1"` in its Parquet
key-value metadata. Within schema v1, changes are additive only — columns
will never be renamed, retyped or removed without a version bump you can
detect from that metadata.

## Cookbook

The 20 slowest spans:

```sql
SELECT name, round(duration_ns / 1e6, 2) AS ms, trace_id
FROM read_parquet('telemetry/traces/**/*.parquet')
ORDER BY ms DESC LIMIT 20;
```

SQL statements ranked by total time spent in them:

```sql
SELECT attributes ->> 'db.query.text' AS sql,
       count(*) AS calls,
       round(sum(duration_ns) / 1e6, 2) AS total_ms
FROM read_parquet('telemetry/traces/**/*.parquet')
WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL
GROUP BY sql ORDER BY total_ms DESC LIMIT 15;
```

Requests per minute:

```sql
SELECT time_bucket(INTERVAL 1 minute, start_time) AS minute, count(*) AS requests
FROM read_parquet('telemetry/traces/**/*.parquet')
WHERE name LIKE 'GET %'
GROUP BY minute ORDER BY minute;
```

One trace as an indented tree (also available as `just trace <trace_id>`):

```sql
WITH RECURSIVE spans AS (
  SELECT * FROM read_parquet('telemetry/traces/**/*.parquet')
  WHERE trace_id = 'PASTE_A_TRACE_ID_HERE'
), tree AS (
  SELECT span_id, name, start_time_unix_nano, duration_ns, 0 AS depth
  FROM spans
  WHERE parent_span_id IS NULL OR parent_span_id NOT IN (SELECT span_id FROM spans)
  UNION ALL
  SELECT s.span_id, s.name, s.start_time_unix_nano, s.duration_ns, t.depth + 1
  FROM spans s JOIN tree t ON s.parent_span_id = t.span_id
)
SELECT repeat('· ', depth) || name AS span, round(duration_ns / 1e6, 3) AS ms
FROM tree ORDER BY start_time_unix_nano;
```

Querying a bucket instead of a directory is the same queries with an S3 glob
— run once:

```sql
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', REGION '...');
-- then
SELECT ... FROM read_parquet('s3://my-bucket/telemetry/traces/**/*.parquet');
```

(Serving the telemetry directory back through Datasette via
datasette-parquet is the natural next trick; it currently fights the 1.0
alphas — see `tickets/05-justfile-demo.md`.)

## Coexistence with other OpenTelemetry setups

This plugin installs a `TracerProvider` only when nobody else has. If a real
provider already exists — `opentelemetry-instrument`, or
[datasette-otel-otlp](https://github.com/datasette/datasette-otel-otlp)
imported first — it **attaches its processor to that provider** instead:
Parquet-alongside-OTLP is a supported combo, and the owner's sampler and
`service.name` apply.

Two honest caveats when running both plugins:

- **Import order matters for the otlp plugin.** Plugin import order is not
  guaranteed; if this plugin happens to import first and installs the
  provider, datasette-otel-otlp sees a foreign provider and exports nothing.
  Parquet files keep appearing either way. If your OTLP export goes quiet
  after installing this plugin, that is why.
- **datasette-otel-otlp installed but unconfigured turns sampling off** for
  the provider it owns, which starves an attached Parquet processor too.
  This plugin prints a loud stderr line when it can see that happened.
  Either configure the otlp endpoint or uninstall the otlp plugin.

## Development

```bash
just test    # test suite (needs the editable datasette checkout on the otel branch)
just demo    # local-directory demo on :8002, 2s flush
just query   # canned DuckDB queries against the demo output
just s3-gateway && just demo-s3 && just query-s3   # the same demo against a live S3 API (versitygw)
```
