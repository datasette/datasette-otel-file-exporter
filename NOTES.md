# datasette-otel-file-exporter

Export the OpenTelemetry spans Datasette emits into files — gzipped NDJSON
by default, Parquet if you ask — in a local directory or any S3-compatible
bucket. No collector, no tracing backend, no extra service: your traces are
just files you own. `jq` reads them, DuckDB queries them, pandas loads them,
Datasette itself can serve them. For a data-exploration tool the payoff is
circular in the best way: the traces of your Datasette become a dataset. The
file layout is borrowed from [celld.dev's telemetry
design](https://celld.dev/docs/telemetry/), which proved the "a fleet with a
bucket has observability with no other service" idea this plugin brings to
Datasette.

**Status: pre-release.** Requires the Datasette 1.0 alpha with OpenTelemetry
support (simonw/datasette PRs #2862–#2864) — no released Datasette emits
these spans yet.

## Install

```bash
datasette install datasette-otel-file-exporter               # ndjson to a directory
datasette install "datasette-otel-file-exporter[parquet]"    # + format: parquet (pyarrow)
datasette install "datasette-otel-file-exporter[obstore]"    # + url: s3:// gs:// az:// (obstore)
datasette install "datasette-otel-file-exporter[parquet,obstore]"
```

The base install has no compiled dependencies beyond what Datasette and the
OpenTelemetry SDK already bring: gzipped NDJSON is stdlib, and a local
directory needs nothing else. Parquet and object storage are extras because
pyarrow and obstore are large wheels that a plugin writing JSON to a
directory has no business dragging in.

## Quickstart

```bash
datasette mydb.db -s plugins.datasette-otel-file-exporter.path ./telemetry
```

Browse a few pages, then (from another terminal — files are immutable once
written, so there is no lock contention with the live server):

```bash
gzip -dc telemetry/traces/*/*/*/*/*.ndjson.gz | jq -c '{name, ms: (.duration_ns / 1e6)}'
```

```
{"name":"datasette.startup","ms":7.72}
{"name":"db.query","ms":1.33}
{"name":"GET /(?P<database>[^\\/\\.]+)/(?P<table>[^\\/\\.]+)(\\.(?P<format>\\w+))?$","ms":30.94}
```

Or ask DuckDB, which reads the gzipped files directly:

```bash
duckdb -c "SELECT name, round(duration_ns / 1e6, 2) AS ms
           FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
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
  datasette-otel-file-exporter:
    path: ./telemetry              # local directory (created if absent)
    # url: s3://my-bucket/telemetry  # ...or any obstore URL: s3:// gs:// az://
    format: ndjson                 # or parquet (needs the [parquet] extra)
    flush_interval_seconds: 10     # roll a new file at most this often
    max_buffer_spans: 10000        # ...or when this many spans are buffered
    service_name: my-datasette     # default: "datasette"
```

- `path` **or** `url`, not both (configuring both is a startup error).
  Neither configured → the plugin stays dormant: one stderr line, no files,
  no recording overhead.
- `format` is one of `ndjson` (default) or `parquet`. An unknown format, or
  a format/URL whose extra is not installed, is a startup error with the
  `pip install` line to fix it — a misconfigured telemetry plugin should not
  quietly drop every batch.
- `service_name` sets the `service.name` resource attribute (ignored when
  `OTEL_SERVICE_NAME` is set, or when another provider owns tracing — see
  Coexistence).
- Spans also flush on shutdown: Ctrl-C does not lose the tail.
- Explicit `OTEL_*` environment variables always beat plugin config.

### Object storage credentials

`url:` needs the `[obstore]` extra. Credentials are **never** plugin config —
`datasette.yaml` gets committed to repos. `url:` stores authenticate through
obstore's native chain: standard environment variables, instance metadata /
IAM roles, with automatic refresh.

AWS S3:

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
datasette mydb.db -s plugins.datasette-otel-file-exporter.url s3://my-bucket/telemetry
```

Any S3-compatible service (Cloudflare R2, Tigris, MinIO, versitygw) is the
same plus an endpoint:

```bash
export AWS_ENDPOINT_URL=https://fly.storage.tigris.dev   # or your R2/MinIO URL
export AWS_ALLOW_HTTP=true                               # only for http:// endpoints
```

On Fly.io with Tigris (`fly storage create`), no extra step is needed: Fly
injects `AWS_ENDPOINT_URL_S3` (plus keys) into the app, and the plugin falls
back to it for `s3://` URLs when `AWS_ENDPOINT_URL` is unset.

Operational notes for buckets: each flush is one network PUT from a
background thread (never the event loop). `flush_interval_seconds: 1`
against S3 is a cost/latency decision you are making. A failed PUT is
retried once, then the batch is dropped with one stderr line — spans are
telemetry, not ledger entries; the plugin never buffers unboundedly toward
an unreachable bucket, and never blocks process exit for long on one.

## File layout

```
<path or url prefix>/traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.ndjson.gz
<path or url prefix>/traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.parquet
```

Hour partitions (UTC, from the flush wall clock); `file_id` is
`<unix_millis>-<random>` so names sort chronologically. Files are written
once and never touched again — local writes go to a temp sibling and are
renamed into place, so a reader mid-glob never sees a partial file. Small
files are expected — DuckDB globs cope; compaction is deliberately out of
scope.

## Schema

One record per span, flat at the top level. The open-ended parts —
`attributes`, `resource`, `events`, `links` — are nested. The record has two
encodings, chosen by `format`; every query in the cookbook below reads both
the same way.

| field | notes |
|---|---|
| `trace_id` | 32 lowercase hex chars (OTLP/JSON style, human-pasteable) |
| `span_id` | 16 hex chars |
| `parent_span_id` | null for root spans |
| `name` | |
| `kind` | `SERVER` / `INTERNAL` / `CLIENT` / ... (SpanKind name, not int) |
| `start_time` | convenience rendering of `start_time_unix_nano`, see per-format notes |
| `start_time_unix_nano` | int, full precision — use this for time math |
| `end_time_unix_nano` | int |
| `duration_ns` | int; derived, but the column every query wants |
| `status_code` | `UNSET` / `OK` / `ERROR` |
| `status_message` | nullable |
| `service_name` | pulled out of resource — the facetable column |
| `scope_name` | instrumentation scope (`datasette`) |
| `attributes` | object; `attributes ->> 'db.query.text'` in DuckDB, `.attributes["db.query.text"]` in jq |
| `resource` | object, full resource attrs (includes service.name again) |
| `events` | array of `{name, timestamp_unix_nano, attributes}` |
| `links` | array of `{trace_id, span_id, attributes}` |

**ndjson** (`.ndjson.gz`): one JSON object per line, gzip. Nested parts are
real JSON objects/arrays (empty arrays when there are none), `start_time` is
an ISO-8601 UTC string with microseconds (`2026-09-03T17:10:26.594728Z`),
and every line starts with `"schema_version": 1` — a JSON file has nowhere
else to carry it. DuckDB's `read_ndjson` infers the nested parts as structs
and the `->>` operator works on them unchanged.

**parquet** (`.parquet`, `[parquet]` extra): the nested parts are JSON
**strings** (`attributes ->> 'key'` reads them identically), empty
`events`/`links` are null, `start_time` is `timestamp[us, UTC]`, and the
schema version is the file's `datasette_otel_file_exporter_schema = "1"`
key-value metadata. Arrow types:

| column | arrow type |
|---|---|
| `trace_id`, `span_id`, `name`, `kind`, `status_code`, `service_name`, `scope_name`, `attributes`, `resource` | string |
| `parent_span_id`, `status_message`, `events`, `links` | string, nullable |
| `start_time` | timestamp[us, UTC] |
| `start_time_unix_nano`, `end_time_unix_nano`, `duration_ns` | int64 |

Within schema v1, changes are additive only, in both encodings — fields will
never be renamed, retyped or removed without a version bump you can detect
from the per-line field or the Parquet metadata.

Not a format here: OTLP/JSON, the encoding the OpenTelemetry Collector's own
file exporter writes and its file receiver reads back. It is deeply nested
and built for machines re-ingesting it, not for `jq` or SQL. If a Collector
round-trip matters to you, it would be a third `format`, not a change to
these two — open an issue.

## Cookbook

Everything below is written against the ndjson default. For Parquet, swap
`read_ndjson('…/*.ndjson.gz')` for `read_parquet('…/*.parquet')` — the SQL
is otherwise identical.

Follow the newest file with jq:

```bash
ls -t telemetry/traces/*/*/*/*/*.ndjson.gz | head -1 | xargs gzip -dc \
  | jq -c 'select(.name == "db.query") | {ms: (.duration_ns / 1e6), sql: .attributes["db.query.text"]}'
```

The 20 slowest spans:

```sql
SELECT name, round(duration_ns / 1e6, 2) AS ms, trace_id
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
ORDER BY ms DESC LIMIT 20;
```

SQL statements ranked by total time spent in them:

```sql
SELECT attributes ->> 'db.query.text' AS sql,
       count(*) AS calls,
       round(sum(duration_ns) / 1e6, 2) AS total_ms
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL
GROUP BY sql ORDER BY total_ms DESC LIMIT 15;
```

Requests per minute — from the nanosecond column, which behaves the same in
both formats and every DuckDB version (whether `read_ndjson` infers the ISO
`start_time` string as a timestamp depends on the DuckDB release, and a naive
timestamp cast to `TIMESTAMPTZ` picks up your session zone):

```sql
SELECT time_bucket(INTERVAL 1 minute, to_timestamp(start_time_unix_nano / 1e9)) AS minute,
       count(*) AS requests
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name LIKE 'GET %'
GROUP BY minute ORDER BY minute;
```

One trace as an indented tree (also available as `just trace <trace_id>`):

```sql
WITH RECURSIVE spans AS (
  SELECT * FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
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
SELECT ... FROM read_ndjson('s3://my-bucket/telemetry/traces/**/*.ndjson.gz');
```

(Serving the telemetry directory back through Datasette via
datasette-parquet is the natural next trick; it currently fights the 1.0
alphas — see `tickets/05-justfile-demo.md`.)

## Coexistence with other OpenTelemetry setups

This plugin installs a `TracerProvider` only when nobody else has. If a real
provider already exists — `opentelemetry-instrument`, or
[datasette-otel-otlp](https://github.com/datasette/datasette-otel-otlp)
imported first — it **attaches its processor to that provider** instead:
files-alongside-OTLP is a supported combo, and the owner's sampler and
`service.name` apply.

Since datasette-otel-otlp's ticket 08 (attach-don't-abdicate), the
combination is symmetric: whichever plugin imports first installs the
provider, the other attaches to it, and both export in either order. A
dormant (endpoint-less) otlp install no longer turns sampling off when
another processor is attached to its provider. One residual caveat: if the
provider this plugin attaches to samples nothing (an agent configured with
an always-off sampler, or a pre-fix otlp build), no spans reach the files —
this plugin prints a loud stderr line when it can see that at startup.

## Development

```bash
just test    # test suite; the dev group installs both extras so every format and store is covered
just demo    # local-directory demo on :8002, 2s flush, ndjson
just jq      # the newest file through jq
just query   # canned DuckDB queries against the demo output
FORMAT=parquet just demo   # the same demo writing Parquet (and FORMAT=parquet just query)
just s3-gateway && just demo-s3 && just query-s3   # the same demo against a live S3 API (versitygw)
```
