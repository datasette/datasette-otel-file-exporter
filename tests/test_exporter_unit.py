"""
Unit-level exporter tests: rolling triggers with a fake clock, write
failures, round-trip fidelity. No Datasette involved.
"""

import time

import duckdb
import obstore
import pytest
from obstore.store import LocalStore, MemoryStore
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    SimpleSpanProcessor,
    SpanExportResult,
)
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import Status, StatusCode

from conftest import make_test_spans
from datasette_otel_parquet.exporter import ParquetSpanExporter


class FakeClock:
    def __init__(self):
        self.now = 1_756_700_000.0

    def __call__(self):
        return self.now


def keys(store):
    return [meta["path"] for meta in obstore.list(store).collect()]


def make_exporter(store=None, **kwargs):
    store = store if store is not None else MemoryStore()
    clock = FakeClock()
    kwargs.setdefault("flush_interval_seconds", 10)
    kwargs.setdefault("max_buffer_spans", 10000)
    exporter = ParquetSpanExporter(
        store, clock=clock, background_flush=False, **kwargs
    )
    return exporter, store, clock


def test_no_file_before_either_trigger():
    exporter, store, clock = make_exporter()
    assert exporter.export(make_test_spans(3)) is SpanExportResult.SUCCESS
    clock.now += 9.9
    exporter.export(make_test_spans(3))
    assert keys(store) == []


def test_interval_roll():
    exporter, store, clock = make_exporter()
    exporter.export(make_test_spans(3))
    clock.now += 10
    exporter.export(make_test_spans(1))
    (key,) = keys(store)
    assert key.startswith("traces/")
    assert key.endswith(".parquet")
    # All four spans landed in the one file
    import io

    import pyarrow.parquet as pq

    data = obstore.get(store, key).bytes().to_bytes()
    assert pq.read_table(io.BytesIO(data)).num_rows == 4
    # Buffer restarts: the next span alone does not roll
    exporter.export(make_test_spans(1))
    assert len(keys(store)) == 1


def test_max_spans_roll():
    exporter, store, clock = make_exporter(max_buffer_spans=5)
    exporter.export(make_test_spans(4))
    assert keys(store) == []
    exporter.export(make_test_spans(1))
    assert len(keys(store)) == 1


def test_force_flush_writes_remainder():
    exporter, store, clock = make_exporter()
    exporter.export(make_test_spans(2))
    assert exporter.force_flush() is True
    assert len(keys(store)) == 1
    # Nothing buffered -> flush writes nothing new
    assert exporter.force_flush() is True
    assert len(keys(store)) == 1


def test_shutdown_writes_remainder():
    exporter, store, clock = make_exporter()
    exporter.export(make_test_spans(2))
    exporter.shutdown()
    assert len(keys(store)) == 1


def test_background_flusher_rolls_while_idle():
    "The BSP never visits an exporter while idle; our own thread must."
    store = MemoryStore()
    exporter = ParquetSpanExporter(
        store, flush_interval_seconds=0.5, background_flush=True
    )
    exporter.export(make_test_spans(2))
    deadline = time.time() + 5
    while not keys(store) and time.time() < deadline:
        time.sleep(0.1)
    exporter.shutdown()
    assert len(keys(store)) == 1


def test_key_layout_hour_partitions():
    exporter, store, clock = make_exporter()
    clock.now = 1_756_746_000.0  # 2025-09-01 17:40:00 UTC
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (key,) = keys(store)
    prefix, filename = key.rsplit("/", 1)
    assert prefix == "traces/2025/09/01/17"
    millis, suffix = filename.removesuffix(".parquet").split("-")
    assert millis == str(int(clock.now * 1000))
    assert len(suffix) == 8


def test_write_failure_drops_batch_logs_once(capsys):
    exporter, _, clock = make_exporter(store=object(), max_buffer_spans=1)
    assert exporter.export(make_test_spans(1)) is SpanExportResult.FAILURE
    assert exporter.export(make_test_spans(1)) is SpanExportResult.FAILURE
    err = capsys.readouterr().err
    assert err.count("dropping batch") == 1
    # A fresh batch is not poisoned by the dropped one
    assert exporter._rows == []


def test_round_trip_fidelity(tmp_path):
    "Unicode, huge values, JSON-ish strings, ERROR status, events - via duckdb."
    collected = InMemorySpanExporter()
    provider = TracerProvider(shutdown_on_exit=False)
    provider.add_span_processor(SimpleSpanProcessor(collected))
    tracer = provider.get_tracer("fidelity")
    big = "x" * 100_000
    with tracer.start_as_current_span("gnarly – span 🦆") as span:
        span.set_attribute("note", "héllo → wörld 🚀")
        span.set_attribute("big", big)
        span.set_attribute("json_ish", '{"nested": [1, 2, {"k": "v"}]}')
        span.set_attribute("counts", [1, 2, 3])
        span.add_event("boom happened", {"detail": "läut 💥"})
        span.set_status(Status(StatusCode.ERROR, "boom 💥"))

    store = LocalStore(prefix=str(tmp_path / "tel"), mkdir=True)
    exporter = ParquetSpanExporter(store, background_flush=False)
    exporter.export(collected.get_finished_spans())
    exporter.force_flush()

    glob_sql = f"read_parquet('{tmp_path}/tel/traces/**/*.parquet')"
    (
        (name, note, big_out, nested, counts, status, message, event_detail),
    ) = duckdb.sql(
        f"""
        SELECT
            name,
            attributes ->> 'note',
            attributes ->> 'big',
            json_extract_string(attributes ->> 'json_ish', '$.nested[2].k'),
            attributes -> 'counts',
            status_code,
            status_message,
            events -> 0 -> 'attributes' ->> 'detail'
        FROM {glob_sql}
        """
    ).fetchall()
    assert name == "gnarly – span 🦆"
    assert note == "héllo → wörld 🚀"
    assert big_out == big
    assert nested == "v"
    assert counts == "[1,2,3]"
    assert status == "ERROR"
    assert message == "boom 💥"
    assert event_detail == "läut 💥"


class TestBuildStoreEndpointFallback:
    "Fly.io Tigris injects AWS_ENDPOINT_URL_S3; obstore only reads AWS_ENDPOINT_URL."

    ENV = ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3")

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for var in self.ENV:
            monkeypatch.delenv(var, raising=False)
        monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test-key")
        monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test-secret")

    def test_s3_fallback_applies(self, monkeypatch):
        from datasette_otel_parquet import _build_store

        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://tigris.test:9000")
        store = _build_store(url="s3://bkt/pfx")
        assert store.config.get("endpoint") == "http://tigris.test:9000"
        # client_options must stay unset (env-driven; see ticket 07's trap)
        assert store.client_options is None

    def test_standard_var_wins_by_omission(self, monkeypatch):
        from datasette_otel_parquet import _build_store

        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://standard.test:9000")
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://tigris.test:9000")
        store = _build_store(url="s3://bkt/pfx")
        # obstore reads AWS_ENDPOINT_URL itself at the client layer; .config
        # only reflects explicitly passed kwargs, so "no endpoint in config"
        # proves we did NOT pass the fallback kwarg over the standard var
        assert store.config.get("endpoint") is None

    def test_no_vars_no_kwarg(self):
        from datasette_otel_parquet import _build_store

        store = _build_store(url="s3://bkt/pfx")
        assert store.config.get("endpoint") is None
