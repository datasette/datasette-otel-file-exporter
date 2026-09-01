# 02 — Provider wiring + coexistence with otlp/agent

Status: done

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

### Measured answers (2026-09-01, opentelemetry-sdk 1.44)

1. **Dormant-otlp hazard: confirmed** (`test_dormant_otlp_starves_attached_processor`).
   Startup trace (sampled before the switch) still lands; everything after otlp's
   `ALWAYS_OFF` swap is dropped. v1 answer: at our `_configure`, sniff the shared
   provider's `sampler.get_description()` and print one loud stderr line when the
   effective decision is AlwaysOff (matching `(AlwaysOffSampler)` /
   `root:AlwaysOffSampler`, NOT the default ParentBased description whose
   `remoteParentNotSampled` arms also say AlwaysOffSampler — that was a real
   false-positive bug during the spike). The warning fires when otlp's `startup()`
   hook runs before ours — the order observed in this environment — and is
   best-effort otherwise; README documents the caveat either way.
2. **Import order: both orders leave THIS plugin working.** otlp-first: we attach to
   its provider, both pipelines export (`test_otlp_first_both_export`).
   Parquet-first: we own the provider and export; **otlp sees a real SDK provider at
   import, goes `foreign`, and exports nothing even when configured**
   (`test_parquet_first_otlp_goes_foreign`) — its known behavior, not ours to fix
   from here; the real fix is a shared-wiring package, out of scope. Honest README
   note: if you run both, and OTLP export goes quiet, import order is why.
3. **Shutdown: confirmed.** Own provider → SDK atexit → `shutdown()` → final write
   (the `--get /` smoke and `test_sigint_flushes_the_tail` both end with files on
   disk). Attached → the owner's `shutdown()`/`force_flush()` iterates all
   registered processors, ours included (SynchronousMultiSpanProcessor).

Two more measured facts that shaped the code, recorded here because they are
SDK-version-dependent:

- `BatchSpanProcessor.force_flush()` drains its queue into `exporter.export()` but
  **never calls the exporter's own `force_flush()`** — and its worker **never calls
  `export()` while the queue is empty**. So the interval roll cannot live on BSP
  cadence alone: after a burst followed by idle, the tail would sit buffered until
  the next request or exit. The exporter therefore runs a small daemon flusher
  thread (writes still never happen on the event loop or a request path).
- `schedule_delay_millis` is constructor-only, but the worker re-reads
  `_batch_processor._schedule_delay` every loop, so `_set_schedule_delay()` retunes
  it at startup-hook time (best-effort across SDK layouts; harmless if internals
  move — the flusher thread still guarantees the interval).

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
