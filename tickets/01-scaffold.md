# 01 — Scaffold the package

Status: done

Bare installable plugin, matching the house style, doing nothing yet.

## Work

- Copy the skeleton from `~/work/simonw/datasette-otel-otlp`: `pyproject.toml` shape,
  setuptools backend, `Framework :: Datasette` classifier, Apache-2.0, uv-managed.
- Package name `datasette-otel-parquet`, module `datasette_otel_parquet`, entry point
  `otel_parquet = "datasette_otel_parquet"`.
- Dependencies: `datasette>=1a37`, `opentelemetry-sdk>=1.37`, `pyarrow`, `obstore`.
  Pin minimums loosely; record the obstore version tested against.
  (Tested against obstore 0.11.1, pyarrow 25.0.1; noted in pyproject.toml.)
- Dev group: `pytest`, `pytest-asyncio` (same asyncio strict-mode pytest config as the
  otlp repo), `duckdb` (used by tests/demos to read files back the way users will).
- `datasette_otel_parquet/__init__.py` with a no-op `@hookimpl def startup()` so the
  plugin loads cleanly.
- `.gitignore` (include `telemetry/` — demo output must never be committed), `LICENSE`,
  one-paragraph placeholder `README.md`, `git init` + first commit.

## Acceptance

- `uv run --with-editable . datasette --get /-/plugins` lists the plugin.
- `uv run pytest` passes with a trivial smoke test.
