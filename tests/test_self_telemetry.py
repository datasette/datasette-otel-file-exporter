"""
The exporter's own telemetry: one ``otel_file_exporter.flush`` span per file
written, and the rule that keeps that span from rolling files of its own.

The unit tests wire the real topology synchronously: the exporter under test
gets a tracer from a throwaway provider whose SimpleSpanProcessor feeds
finished spans straight back into that same exporter. A flush span therefore
lands in the exporter's buffer before ``_write()`` even returns - the
feedback loop with no thread or timing in the way.
"""

import gzip
import json
import os
from typing import Any

import pytest
from conftest import make_test_spans
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import InMemoryMetricReader
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor, SpanExportResult
from opentelemetry.sdk.trace.export.in_memory_span_exporter import (
    InMemorySpanExporter,
)
from opentelemetry.trace import SpanKind, StatusCode
from test_exporter_unit import DictStore, FakeClock, keys

from datasette_otel_file_exporter import registry
from datasette_otel_file_exporter.exporter import (
    ATTR_ENCODE_NS,
    ATTR_ERROR_TYPE,
    ATTR_FILE_KEY,
    ATTR_FILE_SIZE,
    ATTR_FORMAT,
    ATTR_SPANS,
    ATTR_STORE,
    ATTR_TRIGGER,
    FLUSH_SPAN,
    METRIC_FILE_SIZE,
    METRIC_FILES,
    SCOPE_NAME,
    FileSpanExporter,
)
from datasette_otel_file_exporter.stores import LocalDirectoryStore, UrlStore


class FlakyStore(DictStore):
    "Fails every put while ``down`` is set; remembers every attempt."

    def __init__(self):
        super().__init__()
        self.down = False
        self.attempts = []

    def put(self, key, data):
        self.attempts.append(key)
        if self.down:
            raise ConnectionError("bucket unreachable")
        super().put(key, data)


def make_looped_exporter(store=None, meter=None, **kwargs):
    """An exporter whose own flush spans feed straight back into it.

    Returns (exporter, store, clock, collected): ``collected`` is an
    InMemorySpanExporter on the same provider, for inspecting the flush spans
    themselves.
    """
    store = store if store is not None else DictStore()
    clock = FakeClock()
    provider = TracerProvider(shutdown_on_exit=False)
    collected = InMemorySpanExporter()
    provider.add_span_processor(SimpleSpanProcessor(collected))
    kwargs.setdefault("flush_interval_seconds", 10)
    kwargs.setdefault("max_buffer_spans", 10000)
    exporter = FileSpanExporter(
        store,
        clock=clock,
        background_flush=False,
        tracer=provider.get_tracer(SCOPE_NAME),
        meter=meter,
        **kwargs,
    )
    provider.add_span_processor(SimpleSpanProcessor(exporter))
    return exporter, store, clock, collected


def flush_spans(collected):
    return [s for s in collected.get_finished_spans() if s.name == FLUSH_SPAN]


def rows(data):
    return [json.loads(line) for line in gzip.decompress(data).decode().splitlines()]


def own_rows(data):
    return [row for row in rows(data) if row["scope_name"] == SCOPE_NAME]


# --- the span ---------------------------------------------------------------


def test_flush_span_describes_the_file():
    exporter, store, _clock, collected = make_looped_exporter()
    exporter.export(make_test_spans(3))
    assert exporter.force_flush() is True
    (key,) = keys(store)

    (span,) = flush_spans(collected)
    assert span.parent is None
    assert span.kind is SpanKind.INTERNAL
    assert span.status.status_code is StatusCode.OK
    assert span.instrumentation_scope.name == SCOPE_NAME
    attributes = dict(span.attributes)
    assert attributes.pop(ATTR_ENCODE_NS) >= 0
    assert attributes == {
        ATTR_TRIGGER: "force_flush",
        ATTR_FORMAT: "ndjson",
        ATTR_STORE: "unknown",  # DictStore declares no scheme
        ATTR_FILE_KEY: key,
        ATTR_FILE_SIZE: len(store.files[key]),
        ATTR_SPANS: 3,
    }
    # ...and it is now buffered, not written: the file holds only the 3
    assert len(rows(store.files[key])) == 3
    assert len(exporter._records) == 1
    assert exporter._foreign_count == 0


@pytest.mark.parametrize("format", ["ndjson", "parquet"])
def test_format_attribute(format):
    exporter, _store, _clock, collected = make_looped_exporter(format=format)
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (span,) = flush_spans(collected)
    assert span.attributes[ATTR_FORMAT] == format


def test_every_trigger_is_named():
    exporter, _store, clock, collected = make_looped_exporter(max_buffer_spans=3)
    exporter.export(make_test_spans(1))
    clock.now += 10
    exporter.export(make_test_spans(1))  # interval
    exporter.export(make_test_spans(3))  # 3 foreign + 1 own >= 3: max_spans
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    exporter.export(make_test_spans(1))
    exporter.shutdown()
    triggers = [s.attributes[ATTR_TRIGGER] for s in flush_spans(collected)]
    assert triggers == ["interval", "max_spans", "force_flush", "shutdown"]
    assert set(triggers) == set(registry.TRIGGER.values)


# --- the loop, and the rule that breaks it ----------------------------------


def test_own_spans_never_roll_a_file_of_their_own():
    "The feedback loop: idle after one real file, nothing more is written."
    exporter, store, clock, _collected = make_looped_exporter()
    exporter.export(make_test_spans(2))
    clock.now += 10
    exporter.export(make_test_spans(1))
    (first,) = keys(store)
    assert len(exporter._records) == 1  # the flush span for `first`

    # Idle for a long time: the interval trigger and force_flush both see a
    # buffer with nothing foreign in it
    for _ in range(20):
        clock.now += 10
        assert exporter.export([]) is SpanExportResult.SUCCESS
        assert exporter.force_flush() is True
    assert keys(store) == [first]

    # A real span arrives; the interval is measured from ITS arrival, not
    # from the flush span that has been waiting all along
    exporter.export(make_test_spans(1))
    clock.now += 9.9
    exporter.export([])
    assert keys(store) == [first]
    clock.now += 0.1
    exporter.export([])
    first_key, second_key = keys(store)

    # The second file carries the record of the first file's write
    (own,) = own_rows(store.files[second_key])
    assert own["name"] == FLUSH_SPAN
    assert own["attributes"][ATTR_FILE_KEY] == first_key
    assert own["attributes"][ATTR_FILE_SIZE] == len(store.files[first_key])
    assert own["attributes"][ATTR_SPANS] == 3
    assert len(rows(store.files[second_key])) == 2


def test_force_flush_skips_self_only_buffer_but_shutdown_writes_it():
    exporter, store, _clock, _collected = make_looped_exporter()
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (first,) = keys(store)
    assert exporter.force_flush() is True
    assert keys(store) == [first]
    exporter.shutdown()
    # Same fake-clock millisecond, so the names sort by random suffix
    (last,) = set(keys(store)) - {first}
    (own,) = own_rows(store.files[last])
    assert own["attributes"][ATTR_FILE_KEY] == first
    assert own["attributes"][ATTR_TRIGGER] == "force_flush"


def test_outage_leaves_exactly_one_own_span_buffered(capsys):
    """A failed write drops its batch whole - the own span riding in it too -
    so own spans cannot pile up during an outage: after every failure the
    buffer holds exactly the record of that failure, and nothing self-only
    ever rolls, so a downed store costs one put attempt per real batch."""
    store = FlakyStore()
    store.down = True
    exporter, _, _clock, collected = make_looped_exporter(store, max_buffer_spans=3)
    for _ in range(10):
        assert exporter.export(make_test_spans(3)) is SpanExportResult.FAILURE
        assert len(exporter._records) == 1
        assert exporter._foreign_count == 0
    assert store.files == {}
    assert len(store.attempts) == 10
    assert capsys.readouterr().err.count("dropping batch") == 1
    assert all(
        s.status.status_code is StatusCode.ERROR
        and s.attributes[ATTR_ERROR_TYPE] == "ConnectionError"
        and ATTR_FILE_SIZE in s.attributes  # encoding worked, the put did not
        for s in flush_spans(collected)
    )
    # The batches carried the earlier failure records: 3 foreign + 1 own
    assert [s.attributes[ATTR_SPANS] for s in flush_spans(collected)] == [3] + [4] * 9


def test_outage_evidence_lands_after_recovery():
    store = FlakyStore()
    exporter, _, _clock, collected = make_looped_exporter(store)
    store.down = True
    exporter.export(make_test_spans(2))
    assert exporter.force_flush() is False
    store.down = False
    exporter.export(make_test_spans(1))
    assert exporter.force_flush() is True

    (key,) = keys(store)
    (own,) = own_rows(store.files[key])
    assert own["status_code"] == "ERROR"
    assert own["status_message"] == "bucket unreachable"
    assert own["attributes"][ATTR_ERROR_TYPE] == "ConnectionError"
    assert own["attributes"][ATTR_SPANS] == 2
    # A successful write then clears the error log dedupe (existing behavior),
    # and its own span is clean
    ok = [s for s in flush_spans(collected) if s.status.status_code is StatusCode.OK]
    assert len(ok) == 1 and ATTR_ERROR_TYPE not in ok[0].attributes


# --- stores -----------------------------------------------------------------


def test_local_store_scheme(tmp_path):
    exporter, _, _clock, collected = make_looped_exporter(
        LocalDirectoryStore(tmp_path / "tel")
    )
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (span,) = flush_spans(collected)
    assert span.attributes[ATTR_STORE] == "file"
    path = tmp_path / "tel" / span.attributes[ATTR_FILE_KEY]
    assert os.path.getsize(path) == span.attributes[ATTR_FILE_SIZE]


def test_url_store_is_one_request_per_file():
    class FakeObstore:
        def __init__(self):
            self.calls = []

        def put(self, path, data, **kwargs):
            self.calls.append((path, data, kwargs))

    inner = FakeObstore()
    store = UrlStore("s3://bucket/prefix", inner)
    assert store.scheme == "s3"
    store.put("traces/a.ndjson.gz", b"x")
    assert inner.calls == [("traces/a.ndjson.gz", b"x", {"use_multipart": False})]


# --- metrics ----------------------------------------------------------------


def test_metrics_through_the_api_meter():
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(SCOPE_NAME)
    store = FlakyStore()
    exporter, _, _clock, _collected = make_looped_exporter(store, meter=meter)
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (key,) = keys(store)
    store.down = True
    exporter.export(make_test_spans(1))
    exporter.force_flush()

    # Any: a data point's type (sum vs histogram) depends on the metric
    points: dict[tuple, Any] = {}
    metrics_data = reader.get_metrics_data()
    assert metrics_data is not None
    for resource in metrics_data.resource_metrics:
        for scope in resource.scope_metrics:
            assert scope.scope.name == SCOPE_NAME
            for metric in scope.metrics:
                for point in metric.data.data_points:
                    points[
                        (metric.name, dict(point.attributes or {}).get(ATTR_ERROR_TYPE))
                    ] = point
    size = points[(METRIC_FILE_SIZE, None)]
    assert size.count == 1 and size.sum == len(store.files[key])
    assert size.attributes == {ATTR_FORMAT: "ndjson", ATTR_STORE: "unknown"}
    assert points[(METRIC_FILES, None)].value == 1
    assert points[(METRIC_FILES, "ConnectionError")].value == 1


# --- registry conformance ---------------------------------------------------


def test_emitted_spans_match_the_registry():
    telemetry_testing = pytest.importorskip("datasette.telemetry_testing")
    reader = InMemoryMetricReader()
    meter = MeterProvider(metric_readers=[reader]).get_meter(SCOPE_NAME)
    store = FlakyStore()
    exporter, _, clock, collected = make_looped_exporter(
        store, meter=meter, max_buffer_spans=3
    )
    # A workload that exercises every trigger and a failure
    exporter.export(make_test_spans(1))
    clock.now += 10
    exporter.export(make_test_spans(1))
    exporter.export(make_test_spans(3))
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    store.down = True
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    store.down = False
    exporter.export(make_test_spans(1))
    exporter.shutdown()

    finished = collected.get_finished_spans()
    telemetry_testing.assert_spans_conform(
        registry.SPAN_NAMES, finished, scope_name=SCOPE_NAME
    )
    telemetry_testing.assert_spans_covered(
        registry.SPAN_NAMES, finished, scope_name=SCOPE_NAME
    )

    collector = telemetry_testing.MetricsCollector(reader)
    collector.collect()
    telemetry_testing.assert_metrics_conform(
        registry.METRICS, collector, scope_name=SCOPE_NAME
    )
    telemetry_testing.assert_metrics_covered(
        registry.METRICS, collector, scope_name=SCOPE_NAME
    )
    assert {str(m) for m in registry.METRICS} == {METRIC_FILE_SIZE, METRIC_FILES}
