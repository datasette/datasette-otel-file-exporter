# 02 — Provider wiring + coexistence with otlp/agent

Status: todo

Get a `BatchSpanProcessor` (feeding ticket 03's exporter) attached early enough to
catch the `datasette.startup` trace, without fighting whoever else may own the
`TracerProvider`.

## Start from the otlp plugin's measured findings

Read `~/work/simonw/datasette-otel-otlp/tickets/02-provider-wiring.md` ("Measured
answer") and its `__init__.py` before writing code. Established facts to reuse, not
re-derive:

- Spans started through the ProxyTracer before `set_tracer_provider()` are
  non-recording **forever**; `datasette.startup` starts before any hook runs, so the
  provider must be installed at module import.
- Config is only readable from the `startup()` hook → two-phase: import installs the
  provider + processor around a **lazy exporter** (bounded pending buffer); the hook
  resolves config and points the lazy exporter at the real one — or drops everything
  and stops sampling (dormant) when there is no config.
- `service_name` is retrofittable onto the queued startup trace by swapping the
  Resource's `_attributes` (spans hold the Resource by reference).

Adapt those pieces (copy, don't import from the otlp package — no dependency between
the two plugins).

## The new part: attach instead of abdicate

Where the otlp plugin goes `mode="foreign"` and does nothing when a real SDK provider
exists, this plugin should **join it**:

- `trace.get_tracer_provider()` is a Proxy/NoOp provider → install our own (two-phase,
  as above).
- It is a real SDK `TracerProvider` (agent, or datasette-otel-otlp imported first) →
  `provider.add_span_processor(our_processor)` and skip sampler/resource management
  entirely — the owner's sampler and `service.name` apply. Parquet-alongside-OTLP is a
  supported combo, not an error.

### Spike first (~30 min), record answers here

1. **Dormant-otlp hazard.** If datasette-otel-otlp is installed with no endpoint it
   sets its sampler to `ALWAYS_OFF` at startup — a processor attached to *its*
   provider then sees nothing. Confirm by test, then pick the v1 answer (likely:
   detect that we recorded zero spans while configured, print one loud stderr line
   naming the cause; a shared-wiring package is the real fix, out of scope).
2. **Import order.** Entry-point load order between the two plugins is not guaranteed.
   Confirm both orders work (each installs-or-attaches correctly whichever goes
   first).
3. **Shutdown.** When we own the provider, ensure process exit flushes (SDK's atexit
   handles `shutdown()` → our exporter's final write). When attached to a foreign
   provider, confirm its shutdown reaches our processor too.

## Config read in `startup()`

- `path` (str) or, later, `url` (ticket 07). Neither present → dormant: one stderr
  line, `ALWAYS_OFF` sampler *if we own the provider*, detach/no-op processor if not.
- `service_name` (default `"datasette"`), applied only when we own the provider and
  `OTEL_SERVICE_NAME` is unset.
- `flush_interval_seconds`, `max_buffer_spans` → passed to ticket 03's exporter; also
  set the `BatchSpanProcessor` `schedule_delay_millis` to match `flush_interval` so
  spans reach the exporter at least that often.

Env precedence, same rule as the otlp plugin: explicit `OTEL_*` vars beat plugin
config, implemented by omission where possible.

## Acceptance

- `datasette --get /` with `path` configured produces a Parquet file containing the
  full startup trace (`datasette.startup` + children) and the request trace.
- No config → zero files, zero recording overhead beyond the installed provider, one
  log line.
- Both plugins installed, both configured: OTLP receiver gets spans AND Parquet files
  appear, in either import order (parametrized test).
- Under `opentelemetry-instrument` (or a test-installed provider): spans still land in
  Parquet, and the plugin did not replace the provider.
