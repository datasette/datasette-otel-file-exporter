# 06 — README: quickstart, schema reference, privacy

Status: done

Written after ticket 07 landed, so `url:` is documented for real rather than
as "planned". The datasette-parquet `just view` trick did not survive the 1.0
alphas (ticket 05) — the cookbook links the ticket instead of promising it.

## Must contain

- **Quickstart**: install, one `-s` flag, browse, one DuckDB query with real output.
  Under 30 seconds of reading before the payoff.
- **The pitch paragraph**: no collector, no backend; traces are files you own; DuckDB
  / datasette-parquet / pandas all read them. One sentence crediting the celld.dev
  telemetry design as the inspiration for the layout.
- **Config reference**: `path`, `flush_interval_seconds`, `max_buffer_spans`,
  `service_name`; note that `url:` (object storage) is planned (link the issue once
  ticket 07 lands, then document it for real).
- **File layout + schema table**: copy ticket 03's table verbatim; state the schema
  versioning promise (file metadata `datasette_otel_parquet_schema`, additive changes
  only within v1).
- **Cookbook**: 3–4 DuckDB queries from ticket 05, plus "serve it back through
  Datasette" via datasette-parquet if `just view` worked.
- **Privacy, as a headline**: these files contain `db.query.text`, which on a public
  instance is user-supplied SQL; once `url:` exists, shipping that off-box is one
  config line. Parameter *values* are never recorded — only counts. Both stated as
  loudly as `~/projects/datasette/demos/otel/README.md` states them.
- **Coexistence note**: works alongside datasette-otel-otlp and under
  `opentelemetry-instrument`; the dormant-otlp caveat from ticket 02's spike, honestly
  stated.

## Acceptance

- A reader who has never heard of OpenTelemetry can get from install to a DuckDB
  result without leaving the README.
