# 04 — Tests

Status: done

Note on case 5 (coexistence): those tests emit spans through
`trace.get_tracer(...)` against the freshly installed provider rather than
through Datasette requests — datasette's modules bind their module-level
`tracer` to whichever provider was live at the process's first span, a
test-process artifact (see the docstring in `tests/test_coexistence.py`).
End-to-end span flow through real requests is covered by `test_export.py`.

## Bootstrap

OTel globals (`trace.set_tracer_provider`) are set-once per process; steal the reset
approach from `~/work/simonw/datasette-otel-otlp/tests/` (it resets the API's global
state and re-runs `_install()` between tests). Without this, test order silently
determines which provider is live.

Read files back with **duckdb**, not pyarrow — assert against the interface users
will actually use.

## Cases

1. **End to end**: Datasette test client + tmpdir path → hit `/` and a table page →
   `force_flush()` → duckdb reads the files; assert the startup trace, request span
   (`http.route` attribute), and `db.query` spans with `db.query.text`.
2. **Schema contract**: exact column names and arrow types from ticket 03's table, and
   the `datasette_otel_parquet_schema=1` file metadata. This test is the tripwire that
   makes schema changes deliberate.
3. **Dormant**: no plugin config → no files, no provider noise, one log line.
4. **Rolling triggers**: unit-level, fake clock — interval roll, max-spans roll,
   nothing written before either.
5. **Coexistence** (ticket 02's acceptance): parametrized import order with
   datasette-otel-otlp installed; foreign-provider attach path.
6. **Write failure**: store that raises on put → export returns FAILURE, one log
   line, requests keep serving.
7. **Round-trip fidelity**: span with unicode, nested-ish JSON string attribute,
   status ERROR + message.

## Acceptance

- `uv run pytest` green, no ordering sensitivity (`pytest -p no:randomly` not needed,
  or randomization enabled deliberately).
- Tests run against the editable datasette on the otel branch (document in conftest
  or Justfile which branch is required).
