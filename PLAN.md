# datasette-otel-parquet

A Datasette plugin that flushes the OpenTelemetry spans Datasette core emits into
Parquet files every N seconds — a local directory in v1, an S3-compatible bucket (via
[obstore](https://pypi.org/project/obstore/)) as the follow-up. Query them with DuckDB,
datasette-parquet, or anything else that reads Parquet. No collector, no backend, no
extra service.

## Context

Datasette's phase-1 OpenTelemetry stack (simonw/datasette PRs #2862–#2864) ships
`opentelemetry-api` only: core emits spans but installs no provider or exporter. The
sibling plugin `datasette-otel-otlp` (~/work/simonw/datasette-otel-otlp) wires spans to
an OTLP backend — but that assumes you *have* a backend.

This plugin occupies a niche that is nearly empty across the whole OTel ecosystem
(surveyed 2026-09-01): **SDK-side Parquet flushing straight to a file/bucket, no
collector or backend in between**. Prior art is all one step removed:

- [celld.dev](https://celld.dev/docs/telemetry/) is the model: `CELLD_OTEL=1` writes
  traces/logs as Parquet to the state bucket under
  `telemetry/traces/<node>/<yyyy>/<mm>/<dd>/<hh>/<id>.parquet`; "a fleet with a bucket
  has observability with no other service"; DuckDB queries it directly.
- Grafana Tempo stores blocks as Parquet (vParquet) but is a backend service.
- The collector-contrib `parquetexporter` was removed for lack of a maintainer
  (open-telemetry/opentelemetry-collector-contrib#27284); the ask keeps coming back
  (#33807). And it required running a collector anyway.
- Parseable / OpenObserve / InfluxDB 3 / GreptimeDB: Parquet underneath, but all
  services you run.

The pitch in one line: *your traces are just files; bring any query engine* — and for
Datasette users specifically, the traces of a data-exploration tool become a dataset.

**Why there is no feedback loop here.** The self-storage experiment in
`~/projects/datasette/demos/otel/self_storage/` proved that writing spans through
Datasette's own instrumented write path self-sustains (~10–14k spans/s) or deadlocks,
and needs a custom suppressing sampler to tame. Writing a Parquet file never touches
Datasette's write path — no `execute_write`, no event loop, no instrumented anything —
so this plugin needs **no suppression machinery at all**. That structural immunity is a
design constraint: the exporter must never call back into Datasette (rules out e.g.
"also register the directory as a Datasette database" living inside the exporter).

## What it looks like to a user

```bash
datasette install datasette-otel-parquet
datasette mydb.db -s plugins.datasette-otel-parquet.path ./telemetry
```

or in `datasette.yaml`:

```yaml
plugins:
  datasette-otel-parquet:
    path: ./telemetry            # v1: local directory (required; no config → dormant)
    # url: s3://bucket/telemetry # later (ticket 07): any obstore URL; creds from env
    flush_interval_seconds: 10   # roll a new file at most this often
    max_buffer_spans: 10000      # ... or when this many spans are buffered
    service_name: my-datasette   # default: "datasette"
```

Then, at any time (files are immutable once written; no lock contention with a live
server):

```sql
-- duckdb
SELECT name, duration_ns / 1e6 AS ms
FROM read_parquet('telemetry/traces/**/*.parquet')
ORDER BY ms DESC LIMIT 20;
```

## Design decisions (made up front)

- **One write path, obstore from day one.** The exporter builds each Parquet file in
  memory and hands the bytes to an `obstore` store — `LocalStore` for `path:`,
  `from_url(...)` for `url:` later. Local-first is a config default, not a separate
  code path; S3/GCS/Azure become ticket 07's config plumbing, not a rewrite. obstore
  has zero required Python deps and its sync API is fine here (writes happen on the
  exporter's background thread, never the event loop).
- **pyarrow writes the Parquet** (zstd compression). It is a heavy wheel, but it is
  the boring correct choice; revisit `arro3-core` (obstore's lightweight sibling) only
  if install weight becomes a real complaint. Decision recorded in ticket 03.
- **File layout follows celld:** `traces/<yyyy>/<mm>/<dd>/<hh>/<file_id>.parquet`
  under the configured path/prefix, hour partitions from the span **end** time batch
  was flushed at, file id = lexically sortable timestamp + random suffix (ULID-style).
  No `<node>` segment in v1 (single instance); reserve it as an optional `node:`
  config for later fleets.
- **Rolling, not file-per-export.** A `BatchSpanProcessor` calls `export()` per ~512
  spans; naive file-per-call sprays tiny files under load. The exporter buffers rows
  and writes when `flush_interval_seconds` has elapsed or `max_buffer_spans` is hit,
  plus on `force_flush()`/`shutdown()` so ctrl-C loses nothing. Compaction of small
  files is explicitly out of scope (that way lies being a database; DuckDB globs cope).
- **Schema is the public contract** — flat, dumb, readable (ticket 03 freezes it):
  hex-string ids, int64 unix-nanos plus a `timestamp[us, UTC]` convenience column,
  `kind`/`status_code` as strings, dedicated `service_name` column, everything else
  (`attributes`, `resource`, `events`, `links`) as JSON strings in v1. Typed/map
  columns are a v2 conversation; JSON-in-string is queryable enough
  (`attributes ->> 'db.query.text'` in DuckDB) and never loses data.
- **Provider wiring is the otlp plugin's two-phase pattern, adapted.** Import-time
  provider install (a ProxyTracer span started before `set_tracer_provider()` is
  non-recording forever, and `datasette.startup` starts before any hook), startup-hook
  config. One deliberate difference: if a real SDK `TracerProvider` is already
  installed (the agent, or datasette-otel-otlp got there first), this plugin
  **attaches its processor to it** via `add_span_processor()` instead of going
  dormant — parquet-alongside-OTLP is a legitimate combo. Spike + edge cases in
  ticket 02.
- **No config → dormant.** Writing files to a surprise location unprompted is rude.
  One stderr line, sampler off (when we own the provider), nothing on disk.
- **Traces only** in v1. No metrics, no logs, no compaction, no retention (retention
  is stretch ticket 08), no sampling knobs.
- **Privacy is a README headline**: `db.query.text` lands in these files, and on a
  public instance that SQL is user-supplied; `url:` makes shipping it off-box one
  config line. Parameter values are never recorded (only counts). Say both loudly.

## Dev environment gotcha

The spans this plugin stores exist only on the datasette PR branches, not on any
released datasette. All dev/demo commands must run against an editable install of
`~/projects/datasette` **checked out on the otel demo stack** (see
`datasette-otel-otlp/Justfile`, which hardcodes `--with-editable ~/projects/datasette`).
Revisit when phase 1 ships in an alpha.

## Tickets

Work them in order; each is self-contained with acceptance criteria.

| # | Ticket | Status |
|---|--------|--------|
| 01 | [Scaffold the package](tickets/01-scaffold.md) | done |
| 02 | [Provider wiring + coexistence with otlp/agent](tickets/02-provider-wiring.md) | done (spike findings recorded in ticket) |
| 03 | [Schema + ParquetSpanExporter + rolling writer](tickets/03-parquet-exporter.md) | done |
| 04 | [Tests](tickets/04-tests.md) | done |
| 05 | [Justfile: demo + canned DuckDB queries](tickets/05-justfile-demo.md) | done (`just view` dropped — see ticket) |
| 07 | [S3/object storage via obstore `url:`](tickets/07-s3-obstore.md) | done (built before 06 so the README documents `url:` for real; live-tested against versitygw) |
| 06 | [README: quickstart, schema reference, privacy](tickets/06-readme.md) | done |
| 08 | [Stretch: retention pruning](tickets/08-retention.md) | todo (stretch — skip for 0.1) |

## Reference material

- `~/work/simonw/datasette-otel-otlp` — the sibling plugin. Steal: pyproject shape,
  entry point, two-phase provider wiring in `datasette_otel_otlp/__init__.py`
  (`_LazySpanExporter`, `_DeferredSampler`, `_install()`/`_configure()` split and the
  measured findings in its `tickets/02-provider-wiring.md`), test bootstrap for
  resetting OTel globals, Justfile `--with-editable` pattern, CI workflows.
- `~/projects/datasette/demos/otel/self_storage/README.md` — the feedback-loop
  measurements; the reason "never call back into Datasette" is a hard rule here.
- `~/projects/datasette/plans/otel-0831/README.md` — the ecosystem brainstorm this
  slots into.
- [obstore docs](https://developmentseed.org/obstore/latest/) — `LocalStore`,
  `S3Store`/`from_url`, sync `obstore.put(store, path, bytes)`; S3 creds from standard
  env vars with automatic refresh.
- [celld telemetry docs](https://celld.dev/docs/telemetry/) — the layout and the
  DuckDB-queries-the-bucket story this plugin borrows.
