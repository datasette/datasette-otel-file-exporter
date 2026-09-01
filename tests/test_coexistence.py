"""
Ticket 02's coexistence acceptance: both plugins installed, either import
order; a foreign (agent-style) provider; the dormant-otlp hazard.

These tests re-run each plugin's _install() after unwinding OpenTelemetry's
set-once global, simulating the two possible entry-point import orders.
Spans are then emitted through ``trace.get_tracer(...)`` against the fresh
global provider rather than through Datasette requests: datasette's modules
bind their module-level ``tracer`` to whichever provider was live at this
test process's first span, so datasette-emitted spans cannot reach a
provider installed mid-run. That is a test-process artifact only - in a real
process the plugins import before datasette's first span resolves its
ProxyTracer - and what these tests verify is the wiring topology, which the
end-to-end tests cannot (they own the process-wide provider).

Needs datasette-otel-otlp importable (wired by `just test`); skips without it.
"""

import glob
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer

import duckdb
import pytest
from datasette.app import Datasette
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)

import datasette_otel_parquet
from conftest import reset_tracer_state

otlp_plugin = pytest.importorskip("datasette_otel_otlp")


@pytest.fixture
def otlp_receiver():
    "Counts OTLP POSTs; parsing the protobuf is the otlp plugin's own business."
    posts = []

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, *args):
            pass

        def do_POST(self):
            self.rfile.read(int(self.headers.get("Content-Length", 0)))
            posts.append(self.path)
            self.send_response(200)
            self.send_header("Content-Length", "0")
            self.end_headers()

    server = HTTPServer(("127.0.0.1", 0), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    server.posts = posts
    server.endpoint = f"http://127.0.0.1:{server.server_address[1]}"
    yield server
    server.shutdown()


async def run_startup(tel_path, endpoint=None):
    "Let both plugins' startup() hooks resolve config, the real code path."
    plugins = {"datasette-otel-parquet": {"path": str(tel_path)}}
    if endpoint is not None:
        plugins["datasette-otel-otlp"] = {"endpoint": endpoint}
    datasette = Datasette([], memory=True, config={"plugins": plugins})
    await datasette.invoke_startup()
    return datasette


def emit_span(name):
    "A span through the CURRENT global provider (not datasette's bound tracer)."
    with trace.get_tracer("coexistence-test").start_as_current_span(name):
        pass


def parquet_names(tel_path):
    if not glob.glob(f"{tel_path}/traces/**/*.parquet", recursive=True):
        return set()
    rows = duckdb.sql(
        f"SELECT DISTINCT name FROM read_parquet('{tel_path}/traces/**/*.parquet')"
    ).fetchall()
    return {name for (name,) in rows}


def flush_everything():
    # In attached mode _state["provider"] is the shared (foreign) provider,
    # so this drains every processor hanging off it, then rolls our file.
    datasette_otel_parquet._state["provider"].force_flush()
    datasette_otel_parquet._state["exporter"].force_flush()


@pytest.mark.asyncio
async def test_otlp_first_both_export(tmp_path, otlp_receiver):
    reset_tracer_state()
    otlp_plugin._install()
    assert otlp_plugin._state["mode"] == "pending"

    datasette_otel_parquet._install()
    state = datasette_otel_parquet._state
    assert state["owns_provider"] is False
    assert state["provider"] is otlp_plugin._state["provider"]

    tel = tmp_path / "tel"
    await run_startup(tel, endpoint=otlp_receiver.endpoint)
    assert state["mode"] == "active"
    assert otlp_plugin._state["mode"] == "active"

    emit_span("both-pipelines-see-this")
    flush_everything()

    assert "both-pipelines-see-this" in parquet_names(tel)
    assert len(otlp_receiver.posts) >= 1


@pytest.mark.asyncio
async def test_parquet_first_otlp_goes_foreign(tmp_path, otlp_receiver):
    """Measured limitation: when this plugin owns the provider, the otlp
    plugin sees a real SDK provider at import and goes foreign - it exports
    nothing even when configured. Recorded in ticket 02; the real fix is a
    shared-wiring package, out of scope."""
    reset_tracer_state()
    datasette_otel_parquet._install()
    assert datasette_otel_parquet._state["owns_provider"] is True

    otlp_plugin._install()
    assert otlp_plugin._state["mode"] == "foreign"

    tel = tmp_path / "tel"
    await run_startup(tel, endpoint=otlp_receiver.endpoint)

    emit_span("parquet-still-works")
    flush_everything()

    assert "parquet-still-works" in parquet_names(tel)
    assert otlp_receiver.posts == []


@pytest.mark.asyncio
async def test_attaches_to_agent_installed_provider(tmp_path, capsys):
    "Under opentelemetry-instrument-style wiring: attach, never replace."
    reset_tracer_state()
    collected = InMemorySpanExporter()
    agent_provider = TracerProvider(shutdown_on_exit=False)
    agent_provider.add_span_processor(SimpleSpanProcessor(collected))
    trace.set_tracer_provider(agent_provider)

    datasette_otel_parquet._install()
    assert datasette_otel_parquet._state["owns_provider"] is False
    assert trace.get_tracer_provider() is agent_provider

    tel = tmp_path / "tel"
    await run_startup(tel)

    emit_span("agent-and-parquet")
    flush_everything()

    assert trace.get_tracer_provider() is agent_provider
    assert "agent-and-parquet" in parquet_names(tel)
    # The agent's own pipeline still sees everything too
    assert "agent-and-parquet" in {
        span.name for span in collected.get_finished_spans()
    }
    # The default sampler must not trip the starvation warning
    assert "samples nothing" not in capsys.readouterr().err


@pytest.mark.asyncio
async def test_dormant_otlp_starves_attached_processor(tmp_path):
    """The hazard from ticket 02's spike: otlp installed with no endpoint
    sets its sampler to ALWAYS_OFF at startup, so a processor attached to
    its provider records nothing from then on, even though this plugin is
    configured. v1 answer: a loud stderr line when the switched-off sampler
    is visible at our configure time, plus honest README documentation."""
    reset_tracer_state()
    otlp_plugin._install()
    datasette_otel_parquet._install()
    assert datasette_otel_parquet._state["owns_provider"] is False

    tel = tmp_path / "tel"
    await run_startup(tel)  # otlp present but unconfigured
    assert otlp_plugin._state["mode"] == "dormant"
    assert datasette_otel_parquet._state["mode"] == "active"

    emit_span("nobody-sees-this")
    flush_everything()

    assert parquet_names(tel) == set()
