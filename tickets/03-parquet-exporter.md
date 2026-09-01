# 03 — Schema + ParquetSpanExporter + rolling writer

Status: done

Deviation from the sketch below, measured during ticket 02: the interval trigger
cannot rely on `export()` being called (the BatchSpanProcessor never visits an
exporter while its queue is empty), so the exporter runs a small daemon flusher
thread for idle periods. Writes happen on the processor's thread or that flusher
thread — never the event loop, never a request path. `background_flush=False`
disables it for fake-clock unit tests.

The heart of the plugin: a `SpanExporter` that buffers finished spans and rolls them
into Parquet files through an obstore store. This file's schema is the plugin's public
contract — dashboards and canned queries will be written against it — so freeze it
here before coding.

## Schema v1

One row per span. Flat, no nesting, JSON strings for the open-ended parts.

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

Decisions locked in (revisit only in a schema-v2 conversation): hex-string ids over
16-byte binary; strings over enum ints for `kind`/`status_code`; JSON-in-string over
Arrow maps/structs. All three trade file size for query ergonomics, deliberately.

Writer settings: zstd compression, one row group per file (files are small),
`pyarrow.parquet.write_table` into a `pyarrow.BufferOutputStream`, bytes handed to
`obstore.put(store, key, ...)`. Embed a `key_value_metadata` entry
`datasette_otel_parquet_schema = "1"` in each file.

## Rolling writer

`ParquetSpanExporter(store, prefix, flush_interval_seconds=10, max_buffer_spans=10000)`:

- `export(spans)` (called on the BatchSpanProcessor thread): convert each
  `ReadableSpan` to a row dict, append to buffer; if buffer age ≥ interval or size ≥
  max, write a file. All writing happens on this thread — blocking it is fine, it is
  exactly what it is for.
- `force_flush()` / `shutdown()`: write whatever is buffered. Ctrl-C must not lose
  the tail (the SDK calls shutdown via atexit when we own the provider — verified in
  ticket 02).
- File key: `traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.parquet` under the configured
  prefix; hour partition from wall-clock at flush; `file_id` = zero-padded
  `<unix_millis>-<8 hex random>` so names sort chronologically. Files are written
  once and never touched again.
- A write failure (disk full, perms) must not take down the request path: log once
  per distinct error, drop that batch, return `SpanExportResult.FAILURE`.
- **Hard rule:** this module never imports from `datasette` and never touches the
  instance — structural immunity to the self-storage feedback loop (see
  `~/projects/datasette/demos/otel/self_storage/README.md`).

Store construction (v1): `obstore.store.LocalStore(path, mkdir=True)` — check the
actual constructor signature against the installed obstore; create the directory if
absent. Everything is written relative to the store root, so ticket 07 swaps the
constructor, not the writer.

## Acceptance

- One request produces (after flush) a file DuckDB can read:
  `SELECT name, duration_ns FROM read_parquet('.../**/*.parquet')` returns the request
  span and its `db.query` children with sane durations.
- `attributes ->> 'http.route'` and `->> 'db.query.text'` work in DuckDB against real
  emitted spans.
- Buffer rolls on both triggers (interval elapsed, max spans) — unit-test with a fake
  clock; no file appears before either trigger.
- Killing the server (SIGINT) after a request still yields the request's spans on disk.
- A span with unicode/huge attribute values round-trips (JSON, not repr).
