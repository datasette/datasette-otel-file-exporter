"""
Test fixtures: an OpenTelemetry reset plus small helpers.

set_tracer_provider() is once-per-process in OpenTelemetry, and datasette's
telemetry module binds its tracer to whichever provider is global when the
first span resolves - importing this conftest installs the plugin's provider
before the test modules import datasette.app, so every datasette span in the
whole pytest run flows to that one provider. Rather than fight the once-only
semantics with a new provider per test, reset_otel keeps that single provider
and rewinds the plugin's mutable pieces (lazy exporter, deferred sampler,
resource attributes, state machine) between tests.

Requires datasette 1.0a41 or later (the first release with OpenTelemetry
support). Run via `just test` or `uv run pytest`.
Coexistence tests additionally need datasette-otel-otlp importable (wired by
`just test-coexistence`); they skip when it is absent.
"""

import sqlite3

import pytest
from opentelemetry import trace
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, DEFAULT_ON

import datasette_otel_file_exporter
from datasette_otel_file_exporter import _set_schedule_delay

# The provider the plugin installed when this module imported it, plus the
# original resource attributes - the baseline every test starts from.
_SNAPSHOT = dict(datasette_otel_file_exporter._state)
_SNAPSHOT_RESOURCE_ATTRIBUTES = dict(_SNAPSHOT["resource"].attributes)


def reset_tracer_state():
    "Unwind OpenTelemetry's set-once provider global (for provider-game tests)."
    trace._TRACER_PROVIDER = None
    trace._TRACER_PROVIDER_SET_ONCE._done = False


def _quiesce():
    "Point the lazy exporter at nothing and stop sampling new spans."
    exporter = _SNAPSHOT["exporter"]
    exporter.configure(None)
    if _SNAPSHOT["sampler"] is not None:
        _SNAPSHOT["sampler"].set_delegate(ALWAYS_OFF)


@pytest.fixture(autouse=True)
def reset_otel():
    # If the previous test replaced the global provider (the provider-game
    # tests do), point the world back at the plugin's own
    trace._TRACER_PROVIDER = _SNAPSHOT["provider"]
    trace._TRACER_PROVIDER_SET_ONCE._done = True
    state = datasette_otel_file_exporter._state
    state.clear()
    state.update(_SNAPSHOT)
    state["mode"] = "pending"
    state["dormant_logged"] = False

    # Drain spans left queued by the previous test into a discarding exporter,
    # then rearm the lazy exporter as if the startup hook had never run
    exporter = state["exporter"]
    exporter.configure(None)
    state["provider"].force_flush()
    with exporter._lock:
        exporter._configured = False
        exporter._delegate = None
        exporter._pending = []

    if state["sampler"] is not None:
        state["sampler"].set_delegate(DEFAULT_ON)
    state["resource"]._attributes = BoundedAttributes(
        attributes=_SNAPSHOT_RESOURCE_ATTRIBUTES, immutable=True
    )
    _set_schedule_delay(state["processor"], 5000)
    yield
    # Quiesce so nothing keeps writing into this test's (now gone) tmpdir,
    # and stop any otlp exporter a coexistence test configured before its
    # receiver goes away
    _quiesce()
    try:
        import datasette_otel_otlp  # ty: ignore[unresolved-import] - optional

        if "exporter" in datasette_otel_otlp._state:
            datasette_otel_otlp._state["exporter"].configure(None)
    except ImportError:
        pass


def flush():
    """Push every finished span through to files.

    Two stages because the BatchSpanProcessor's force_flush only drains its
    queue into exporter.export() - it never calls the exporter's own
    force_flush - and export() buffers rows until a roll trigger fires.
    """
    datasette_otel_file_exporter._state["provider"].force_flush()
    datasette_otel_file_exporter._state["exporter"].force_flush()


def make_test_spans(count=1, name="span"):
    "ReadableSpans from a throwaway provider, for unit-level exporter tests."
    collected = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(collected))
    tracer = provider.get_tracer("test")
    for i in range(count):
        with tracer.start_as_current_span(f"{name}-{i}"):
            pass
    return list(collected.get_finished_spans())


@pytest.fixture
def demo_db(tmp_path):
    path = tmp_path / "demo.db"
    connection = sqlite3.connect(path)
    connection.execute(
        "create table plants(id integer primary key, name text, height_cm real)"
    )
    connection.executemany(
        "insert into plants values (?, ?, ?)",
        [(i, f"plant {i}", float(i)) for i in range(1, 51)],
    )
    connection.commit()
    connection.close()
    return path
