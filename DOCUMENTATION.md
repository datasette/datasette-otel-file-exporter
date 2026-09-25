# `datasette-otel-file-exporter` Documentation

- [Export File Format](#export-file-format)
  - [File layout](#file-layout)
  - [Schema](#schema)
  - [NDJSON](#ndjson)
  - [Parquet](#parquet)
- [Configuration Reference](#configuration-reference)
  - [Options](#options)
  - [Using S3](#using-s3)
- [Privacy](#privacy)
- [The exporter's own telemetry](#the-exporters-own-telemetry)
- [Using alongside other OpenTelemetry setups](#using-alongside-other-opentelemetry-setups)

## Export File Format

### File layout

Files are written under the configured `path` directory or `url` prefix, partitioned by hour (UTC):

```
<prefix>/traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.ndjson.gz
<prefix>/traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.parquet
```

The `file_id` is `<unix_millis>-<random>`, so file names sort chronologically. The layout is borrowed from [celld.dev's telemetry design](https://celld.dev/docs/telemetry/).

A file is written once and never modified. Local writes go to a temporary file that is then renamed into place, so you can safely read the directory while Datasette is running and you'll never see a half-written file.

Expect lots of small files. DuckDB and similar tools handle globs over many files fine, and the plugin doesn't try to compact them.

### Schema

Each span becomes one record. Most fields are flat columns; `attributes`, `resource`, `events` and `links` are nested.

| field | notes |
|---|---|
| `trace_id` | 32 lowercase hex characters |
| `span_id` | 16 lowercase hex characters |
| `parent_span_id` | `null` for root spans |
| `name` | Span name, e.g. `db.query` |
| `kind` | `SERVER`, `INTERNAL`, `CLIENT`, ... |
| `start_time` | Human-readable start time (see the per-format notes below) |
| `start_time_unix_nano` | Integer, full precision. Use this one for time math |
| `end_time_unix_nano` | Integer |
| `duration_ns` | Integer, `end - start` |
| `status_code` | `UNSET`, `OK` or `ERROR` |
| `status_message` | Nullable |
| `service_name` | Copied out of `resource` so it's easy to filter on |
| `scope_name` | Instrumentation scope, usually `datasette` |
| `attributes` | Span attributes, e.g. `db.query.text` |
| `resource` | All resource attributes (including `service.name`) |
| `events` | Array of `{name, timestamp_unix_nano, attributes}` |
| `links` | Array of `{trace_id, span_id, attributes}` |

In DuckDB, `attributes ->> 'db.query.text'` works the same way against both formats. In `jq` it's `.attributes["db.query.text"]`.

Schema changes within version 1 will only ever add fields. Renaming, retyping or removing a field will bump the schema version, which you can read from each NDJSON line or from the Parquet file metadata.

### NDJSON

The default. Files end in `.ndjson.gz` and contain one JSON object per line, gzip-compressed.

- Every line includes `"schema_version": 1`.
- `attributes`, `resource`, `events` and `links` are real JSON objects and arrays. `events` and `links` are `[]` when empty.
- `start_time` is an ISO-8601 UTC string with microseconds, like `2026-09-03T17:10:26.594728Z`.

DuckDB's `read_ndjson()` reads the gzipped files directly and infers the nested fields as structs.

Only the stdlib is needed to write NDJSON, so it works with the base install.

### Parquet

Set `format: parquet` and install the `[parquet]` extra (which pulls in `pyarrow`). Files end in `.parquet`.

- `attributes`, `resource`, `events` and `links` are stored as JSON strings. `events` and `links` are `null` when empty.
- `start_time` is a `timestamp[us, UTC]`.
- The schema version is stored in the file's key-value metadata as `datasette_otel_file_exporter_schema = "1"`.

| column | Arrow type |
|---|---|
| `trace_id`, `span_id`, `name`, `kind`, `status_code`, `service_name`, `scope_name`, `attributes`, `resource` | `string` |
| `parent_span_id`, `status_message`, `events`, `links` | `string`, nullable |
| `start_time` | `timestamp[us, UTC]` |
| `start_time_unix_nano`, `end_time_unix_nano`, `duration_ns` | `int64` |

OTLP/JSON (the format the OpenTelemetry Collector's file exporter writes) isn't supported. It's deeply nested and awkward to query with `jq` or SQL. If you need to round-trip through a Collector, please [open an issue](https://github.com/datasette/datasette-otel-file-exporter/issues).

## Configuration Reference

### Options

```yaml
# datasette.yaml
plugins:
  datasette-otel-file-exporter:
    path: ./telemetry
    format: ndjson
    flush_interval_seconds: 10
    max_buffer_spans: 10000
    service_name: my-datasette
```

| option | default | description |
|---|---|---|
| `path` | | Local directory to write to. Created if it doesn't exist. |
| `url` | | Object storage URL to write to instead, e.g. `s3://my-bucket/telemetry`. `gs://` and `az://` also work. Requires the `[obstore]` extra. |
| `url_config` | | Store configuration for `url`, including credentials. See [Using S3](#using-s3). |
| `format` | `ndjson` | `ndjson` or `parquet`. Parquet requires the `[parquet]` extra. |
| `flush_interval_seconds` | `10` | Write a new file at most this often. |
| `max_buffer_spans` | `10000` | Write a new file early once this many spans are buffered. |
| `service_name` | `datasette` | The `service.name` resource attribute. Ignored if `OTEL_SERVICE_NAME` is set, or if another OpenTelemetry provider is already in charge. |

Some behavior to be aware of:

- Set `path` or `url`, not both. Setting both is a startup error.
- If neither is set, the plugin does nothing. It prints one line to stderr and adds no overhead.
- An unknown `format`, or a format or URL whose extra isn't installed, fails at startup with the `pip install` command you need.
- Buffered spans are written on shutdown, so Ctrl-C doesn't lose the last few seconds.
- `OTEL_*` environment variables always take precedence over plugin configuration.

### Using S3

Writing to object storage needs the `[obstore]` extra. Uploads use [obstore](https://developmentseed.org/obstore/), which supports S3, Google Cloud Storage and Azure.

#### Credentials from the environment

Without `url_config`, credentials come from obstore's standard chain: environment variables, then instance metadata or IAM roles.

```bash
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-east-1
datasette mydb.db -s plugins.datasette-otel-file-exporter.url s3://my-bucket/telemetry
```

For S3-compatible services like Cloudflare R2, Tigris or MinIO, also set the endpoint:

```bash
export AWS_ENDPOINT_URL=https://fly.storage.tigris.dev
export AWS_ALLOW_HTTP=true   # only needed for http:// endpoints
```

On Fly.io, `fly storage create` sets `AWS_ENDPOINT_URL_S3` along with the keys. The plugin uses that for `s3://` URLs when `AWS_ENDPOINT_URL` isn't set, so no extra configuration is needed.

#### Credentials in configuration

To keep everything in `datasette.yaml`, or to use your own environment variable names, use `url_config`. Use Datasette's `$env` or `$file` so secrets never appear in the file itself:

```yaml
plugins:
  datasette-otel-file-exporter:
    url: s3://my-bucket/telemetry
    url_config:
      access_key_id:
        $env: TELEMETRY_S3_KEY_ID
      secret_access_key:
        $env: TELEMETRY_S3_SECRET
      endpoint: https://<account>.r2.cloudflarestorage.com
      region: auto
```

```bash
TELEMETRY_S3_KEY_ID=... TELEMETRY_S3_SECRET=... datasette mydb.db -c datasette.yaml
```

`url_config` is passed straight to obstore as the store config, so the keys are obstore's: `access_key_id`, `secret_access_key`, `session_token`, `endpoint`, `region` and so on for [S3](https://developmentseed.org/obstore/latest/api/store/aws/); `service_account_key` for GCS; `account_key` or `sas_key` for Azure.

- Keys you set override the matching environment variables. Keys you leave out still come from the environment.
- If an `$env` variable isn't set, that key is skipped (with a note on stderr) and the environment chain fills it in. This means the same config works locally with explicit keys and on a server using an IAM role.
- An unknown key disables export, with a stderr line naming the key.
- `url_config` without `url` is a startup error.
- `allow_http` isn't a store option. Plain `http://` endpoints still need `AWS_ALLOW_HTTP=true` in the environment.

#### Uploads and failures

Each file is uploaded with a single `PutObject` request from a background thread, so uploads never block Datasette's event loop. The file count is also your request count, which helps when estimating costs. A low `flush_interval_seconds` means more requests.

If an upload fails it is retried once. If it fails again, that batch of spans is dropped and a line is printed to stderr. The plugin won't buffer indefinitely while a bucket is unreachable, and won't hold up shutdown waiting on one.

## Privacy

The exported files contain SQL. Datasette records the text of each query in `db.query.text` (truncated), and on a public instance that includes whatever SQL visitors send via `?sql=`. Parameter values are never recorded, only the number of parameters, but table names, column names and query shapes are all there.

Treat the telemetry directory or bucket with the same care as the database it describes.

## The exporter's own telemetry

Every file the plugin writes is recorded as an `otel_file_exporter.flush` span, with `scope_name = 'datasette_otel_file_exporter'`. That span is written into the next file, so you can use the files to see how the exporter itself is doing: bytes and files per hour, upload latency, failures.

To leave these out of your queries, add `WHERE scope_name != 'datasette_otel_file_exporter'`.

| attribute | description |
|---|---|
| `otel_file_exporter.trigger` | Why the file was written: `interval`, `max_spans`, `force_flush` or `shutdown` |
| `otel_file_exporter.format` | `ndjson` or `parquet` |
| `otel_file_exporter.store` | `file` for a directory, otherwise the URL scheme (`s3`, `gs`, `az`) |
| `otel_file_exporter.file.key` | The file's key under the path or URL prefix |
| `otel_file_exporter.file.size_bytes` | Compressed file size |
| `otel_file_exporter.spans` | Number of records in the file |
| `otel_file_exporter.encode_ns` | Time spent encoding. `duration_ns - encode_ns` is roughly the upload time |
| `error.type` | Set on failure to the exception class. The span status is `ERROR` and the batch was dropped |

Some details:

- Since every upload is exactly one `PutObject`, successful flush spans give you an exact count of PUT requests, and the sum of `file.size_bytes` is the bytes uploaded (plus at most one retry per failure).
- When a write fails, the flush span describing it shows up in the first file written after the store recovers. During a long outage only the most recent failure is kept.
- A buffer containing only flush spans never triggers a write on its own. Otherwise an idle server would write a tiny file every `flush_interval_seconds` forever. They're held until real spans arrive, or until shutdown.
- The same numbers are also reported as OpenTelemetry metrics, `otel_file_exporter.file.size` (histogram) and `otel_file_exporter.files` (counter). These do nothing unless you've configured a `MeterProvider`. This plugin doesn't write metrics to files.

## Using alongside other OpenTelemetry setups

The plugin only installs its own `TracerProvider` if no other provider exists. If one does, for example from `opentelemetry-instrument` or [datasette-otel-otlp](https://github.com/datasette/datasette-otel-otlp), it adds its processor to that provider instead. That means you can write files and send OTLP to a backend at the same time. In that case the other provider's sampler and `service.name` apply.

With datasette-otel-otlp, load order doesn't matter: whichever plugin loads first creates the provider and the other attaches to it.

If the provider it attaches to never samples anything (say an agent configured with an always-off sampler), no spans will reach the files. The plugin prints a warning at startup when it can detect this.
