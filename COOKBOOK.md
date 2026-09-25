# `datasette-otel-file-exporter` Cookbook

> [!WARNING] 
> Heavy use of agents here, double check your work!

Recipes for querying exported traces with `jq` and [DuckDB](https://duckdb.org/). They all assume the default NDJSON format in a `telemetry/` directory.

For Parquet, replace `read_ndjson('telemetry/traces/**/*.ndjson.gz')` with `read_parquet('telemetry/traces/**/*.parquet')`. The rest of the SQL stays the same.

- [Quick look with jq](#quick-look-with-jq)
- [Slowest spans](#slowest-spans)
- [SQL queries by total time](#sql-queries-by-total-time)
- [Requests per minute](#requests-per-minute)
- [A single trace as a tree](#a-single-trace-as-a-tree)
- [Querying files in S3](#querying-files-in-s3)
- [Monitoring the exporter](#monitoring-the-exporter)

## Quick look with jq

Every span, with its duration in milliseconds:

```bash
gzip -dc telemetry/traces/*/*/*/*/*.ndjson.gz | jq -c '{name, ms: (.duration_ns / 1e6)}'
```

```
{"name":"datasette.startup","ms":7.72}
{"name":"db.query","ms":1.33}
{"name":"GET /(?P<database>[^\\/\\.]+)/(?P<table>[^\\/\\.]+)(\\.(?P<format>\\w+))?$","ms":30.94}
```

SQL queries from the most recent file:

```bash
ls -t telemetry/traces/*/*/*/*/*.ndjson.gz | head -1 | xargs gzip -dc \
  | jq -c 'select(.name == "db.query") | {ms: (.duration_ns / 1e6), sql: .attributes["db.query.text"]}'
```

## Slowest spans

```sql
SELECT name, round(duration_ns / 1e6, 2) AS ms, trace_id
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
ORDER BY ms DESC LIMIT 20;
```

## SQL queries by total time

Which queries Datasette spends the most time running, across all requests:

```sql
SELECT attributes ->> 'db.query.text' AS sql,
       count(*) AS calls,
       round(sum(duration_ns) / 1e6, 2) AS total_ms
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL
GROUP BY sql ORDER BY total_ms DESC LIMIT 15;
```

## Requests per minute

```sql
SELECT time_bucket(INTERVAL 1 minute, to_timestamp(start_time_unix_nano / 1e9)) AS minute,
       count(*) AS requests
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name LIKE 'GET %'
GROUP BY minute ORDER BY minute;
```

This uses `start_time_unix_nano` rather than `start_time`. Whether DuckDB parses the NDJSON `start_time` string as a timestamp depends on the DuckDB version, and the integer column behaves the same everywhere.

## A single trace as a tree

Pick a `trace_id` from one of the queries above:

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

## Querying files in S3

DuckDB can read straight from a bucket. Create a secret once:

```sql
CREATE SECRET (TYPE s3, KEY_ID '...', SECRET '...', REGION '...');
```

Then use an `s3://` glob in any of the queries above:

```sql
SELECT count(*) FROM read_ndjson('s3://my-bucket/telemetry/traces/**/*.ndjson.gz');
```

## Monitoring the exporter

The plugin records each file it writes as an `otel_file_exporter.flush` span (see [the exporter's own telemetry](./DOCUMENTATION.md#the-exporters-own-telemetry)).

Files, bytes and spans written per hour, with 95th percentile upload time:

```sql
SELECT time_bucket(INTERVAL 1 hour, to_timestamp(start_time_unix_nano / 1e9)) AS hour,
       count(*) AS files,
       sum((attributes ->> 'otel_file_exporter.file.size_bytes')::BIGINT) AS bytes,
       sum((attributes ->> 'otel_file_exporter.spans')::BIGINT) AS spans,
       round(quantile_cont(duration_ns - (attributes ->> 'otel_file_exporter.encode_ns')::BIGINT, 0.95) / 1e6, 1) AS put_p95_ms
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name = 'otel_file_exporter.flush' AND status_code = 'OK'
GROUP BY hour ORDER BY hour;
```

Batches that failed to upload and were dropped:

```sql
SELECT start_time, attributes ->> 'error.type' AS error, status_message,
       (attributes ->> 'otel_file_exporter.spans')::BIGINT AS spans_lost
FROM read_ndjson('telemetry/traces/**/*.ndjson.gz')
WHERE name = 'otel_file_exporter.flush' AND status_code = 'ERROR'
ORDER BY start_time_unix_nano;
```
