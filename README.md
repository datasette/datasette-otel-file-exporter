# datasette-otel-parquet

**Status: under development.** Not yet released.

Flush the OpenTelemetry spans Datasette emits into Parquet files — a local directory
first, an S3-compatible bucket later — and query them with DuckDB or Datasette itself.
No collector, no tracing backend, no extra service.

Start with [PLAN.md](PLAN.md); work the [tickets](tickets/) in order.
