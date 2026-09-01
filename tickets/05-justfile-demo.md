# 05 — Justfile: demo + canned DuckDB queries

Status: done

`just view` (datasette-parquet) tried and dropped: against the 1.0 alpha its
SQL shim raises `DoubleQuoteForLiteraValue` on datasette's own internal
queries (double-quoted literals rewrite), 500ing every page (tested
2026-09-01, datasette-parquet from PyPI + editable datasette otel branch).
Revisit when datasette-parquet catches up with 1.0; DuckDB recipes cover the
payoff meanwhile.

Make the payoff visible in two commands. Copy the Justfile conventions from
`~/work/simonw/datasette-otel-otlp/Justfile`, including the
`--with-editable ~/projects/datasette` gotcha (spans only exist on the otel branches).

## Recipes

- `just demo` — serve `demo.db` (grab the one from `~/projects/datasette/demos/otel/`)
  with `path: ./telemetry`, short flush interval (2s) so files appear fast.
- `just query` — the money shot, DuckDB CLI over `telemetry/traces/**/*.parquet`:
  slowest spans; requests per minute; `db.query.text` ranked by total time. Keep each
  as a separate recipe (`just slowest`, `just sql-hotspots`) or one script — whichever
  reads better in the README.
- `just trace <trace_id>` — one trace's spans ordered by start time, indented by
  depth if cheap to do in SQL (recursive CTE on parent_span_id), plain ordered list if
  not.
- `just view` — stretch: serve the telemetry directory back through Datasette via
  datasette-parquet, the "traces are just another dataset" demo. If datasette-parquet
  fights the 1.0 alphas, note that and drop it.

## Acceptance

- Fresh checkout: `just demo` in one terminal, browse a few pages, `just query` in
  another prints something a screenshot could sell.
- `just clean` deletes `./telemetry`.
