"""
Unit-level exporter tests: rolling triggers with a fake clock, write
failures, round-trip fidelity in both formats, the local store, the format
and store dependency checks. No Datasette involved.
"""

import gzip
import io
import json
import sys
import time

import duckdb
import pytest
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
from datasette_otel_file_exporter.exporter import (
    FileSpanExporter,
    FormatUnavailable,
    check_format,
)
from datasette_otel_file_exporter.stores import (
    LocalDirectoryStore,
    ObstoreUnavailable,
)

FORMATS = ["ndjson", "parquet"]
SUFFIX = {"ndjson": ".ndjson.gz", "parquet": ".parquet"}
READER = {"ndjson": "read_ndjson", "parquet": "read_parquet"}


def read_sql(tel_dir, format, sql):
    "duckdb over the exported files; $T is the table expression."
    table = f"{READER[format]}('{tel_dir}/traces/**/*{SUFFIX[format]}')"
    return duckdb.sql(sql.replace("$T", table)).fetchall()


class FakeClock:
    def __init__(self):
        self.now = 1_756_700_000.0

    def __call__(self):
        return self.now


class DictStore:
    "The whole store interface the exporter needs: put(key, bytes)."

    def __init__(self):
        self.files = {}

    def put(self, key, data):
        self.files[key] = data


def keys(store):
    return sorted(store.files)


def row_count(data, format):
    if format == "ndjson":
        return len(gzip.decompress(data).decode().splitlines())
    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(data)).num_rows


def make_exporter(store=None, **kwargs):
    store = store if store is not None else DictStore()
    clock = FakeClock()
    kwargs.setdefault("flush_interval_seconds", 10)
    kwargs.setdefault("max_buffer_spans", 10000)
    exporter = FileSpanExporter(
        store, clock=clock, background_flush=False, **kwargs
    )
    return exporter, store, clock


def test_no_file_before_either_trigger():
    exporter, store, clock = make_exporter()
    assert exporter.export(make_test_spans(3)) is SpanExportResult.SUCCESS
    clock.now += 9.9
    exporter.export(make_test_spans(3))
    assert keys(store) == []


@pytest.mark.parametrize("format", FORMATS)
def test_interval_roll(format):
    exporter, store, clock = make_exporter(format=format)
    exporter.export(make_test_spans(3))
    clock.now += 10
    exporter.export(make_test_spans(1))
    (key,) = keys(store)
    assert key.startswith("traces/")
    assert key.endswith(SUFFIX[format])
    # All four spans landed in the one file
    assert row_count(store.files[key], format) == 4
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
    store = DictStore()
    exporter = FileSpanExporter(
        store, flush_interval_seconds=0.5, background_flush=True
    )
    exporter.export(make_test_spans(2))
    deadline = time.time() + 5
    while not keys(store) and time.time() < deadline:
        time.sleep(0.1)
    exporter.shutdown()
    assert len(keys(store)) == 1


@pytest.mark.parametrize("format", FORMATS)
def test_key_layout_hour_partitions(format):
    exporter, store, clock = make_exporter(format=format)
    clock.now = 1_756_746_000.0  # 2025-09-01 17:40:00 UTC
    exporter.export(make_test_spans(1))
    exporter.force_flush()
    (key,) = keys(store)
    prefix, filename = key.rsplit("/", 1)
    assert prefix == "traces/2025/09/01/17"
    millis, suffix = filename.removesuffix(SUFFIX[format]).split("-")
    assert millis == str(int(clock.now * 1000))
    assert len(suffix) == 8


def test_write_failure_drops_batch_logs_once(capsys):
    exporter, _, clock = make_exporter(store=object(), max_buffer_spans=1)
    assert exporter.export(make_test_spans(1)) is SpanExportResult.FAILURE
    assert exporter.export(make_test_spans(1)) is SpanExportResult.FAILURE
    err = capsys.readouterr().err
    assert err.count("dropping batch") == 1
    # A fresh batch is not poisoned by the dropped one
    assert exporter._records == []


def gnarly_spans():
    "Unicode, huge values, JSON-ish strings, ERROR status, events."
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
    return collected.get_finished_spans(), big


@pytest.mark.parametrize("format", FORMATS)
def test_round_trip_fidelity(tmp_path, format):
    "The same duckdb query reads both encodings back identically."
    spans, big = gnarly_spans()
    exporter = FileSpanExporter(
        LocalDirectoryStore(tmp_path / "tel"), format=format, background_flush=False
    )
    exporter.export(spans)
    exporter.force_flush()

    (
        (name, note, big_out, nested, counts, status, message, event_detail),
    ) = read_sql(
        tmp_path / "tel",
        format,
        """
        SELECT
            name,
            attributes ->> 'note',
            attributes ->> 'big',
            json_extract_string(attributes ->> 'json_ish', '$.nested[2].k'),
            attributes -> 'counts',
            status_code,
            status_message,
            events -> 0 -> 'attributes' ->> 'detail'
        FROM $T
        """,
    )
    assert name == "gnarly – span 🦆"
    assert note == "héllo → wörld 🚀"
    assert big_out == big
    assert nested == "v"
    assert counts == "[1,2,3]"
    assert status == "ERROR"
    assert message == "boom 💥"
    assert event_detail == "läut 💥"


def test_ndjson_line_shape():
    "What jq sees: nested objects, ISO start_time, schema_version on every line."
    spans, _ = gnarly_spans()
    exporter, store, clock = make_exporter()
    exporter.export(spans)
    exporter.force_flush()
    (key,) = keys(store)
    (line,) = gzip.decompress(store.files[key]).decode("utf-8").splitlines()
    row = json.loads(line)
    assert list(row) == [
        "schema_version",
        "trace_id",
        "span_id",
        "parent_span_id",
        "name",
        "kind",
        "start_time",
        "start_time_unix_nano",
        "end_time_unix_nano",
        "duration_ns",
        "status_code",
        "status_message",
        "service_name",
        "scope_name",
        "attributes",
        "resource",
        "events",
        "links",
    ]
    assert row["schema_version"] == 1
    assert row["attributes"]["counts"] == [1, 2, 3]
    assert row["attributes"]["note"] == "héllo → wörld 🚀"
    assert row["events"][0]["attributes"]["detail"] == "läut 💥"
    assert row["links"] == []
    assert row["parent_span_id"] is None
    # start_time is the ISO rendering of start_time_unix_nano at µs precision
    micros = row["start_time_unix_nano"] // 1000
    from datetime import datetime, timedelta, timezone

    expected = datetime(1970, 1, 1, tzinfo=timezone.utc) + timedelta(
        microseconds=micros
    )
    assert row["start_time"] == expected.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    # Non-ASCII survives as itself, not \\u escapes
    assert "🦆" in line


def test_ndjson_is_deterministic():
    spans, _ = gnarly_spans()
    from datasette_otel_file_exporter.exporter import encode_ndjson, span_to_record

    records = [span_to_record(span) for span in spans]
    assert encode_ndjson(records) == encode_ndjson(records)


class TestFormatChecks:
    def test_unknown_format(self):
        with pytest.raises(ValueError, match="unknown format 'csv'"):
            check_format("csv")
        with pytest.raises(ValueError):
            FileSpanExporter(DictStore(), format="csv", background_flush=False)

    def test_parquet_without_pyarrow(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        with pytest.raises(FormatUnavailable, match=r"\[parquet\]"):
            check_format("parquet")

    def test_ndjson_needs_nothing(self, monkeypatch):
        monkeypatch.setitem(sys.modules, "pyarrow", None)
        check_format("ndjson")


class TestLocalDirectoryStore:
    def test_put_creates_partitions_atomically(self, tmp_path):
        store = LocalDirectoryStore(tmp_path / "tel")
        store.put("traces/2025/09/01/17/abc.ndjson.gz", b"hello")
        target = tmp_path / "tel/traces/2025/09/01/17/abc.ndjson.gz"
        assert target.read_bytes() == b"hello"
        # No temp file left behind, nothing else in the partition
        assert [p.name for p in target.parent.iterdir()] == ["abc.ndjson.gz"]

    def test_root_created(self, tmp_path):
        LocalDirectoryStore(tmp_path / "a" / "b")
        assert (tmp_path / "a" / "b").is_dir()

    def test_unusable_root_raises(self, tmp_path):
        blocker = tmp_path / "blocker"
        blocker.write_text("a file, not a directory")
        with pytest.raises(OSError):
            LocalDirectoryStore(blocker / "tel")


class TestBuildStore:
    def test_path_is_local(self, tmp_path):
        from datasette_otel_file_exporter import _build_store

        store = _build_store(path=str(tmp_path / "tel"))
        assert isinstance(store, LocalDirectoryStore)

    def test_url_without_obstore(self, monkeypatch):
        from datasette_otel_file_exporter.stores import check_obstore

        monkeypatch.setitem(sys.modules, "obstore", None)
        with pytest.raises(ObstoreUnavailable, match=r"\[obstore\]"):
            check_obstore()


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
        from datasette_otel_file_exporter import _build_store

        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://tigris.test:9000")
        store = _build_store(url="s3://bkt/pfx")
        assert store.inner.config.get("endpoint") == "http://tigris.test:9000"
        # client_options must stay unset (env-driven; see ticket 07's trap)
        assert store.inner.client_options is None

    def test_standard_var_wins_by_omission(self, monkeypatch):
        from datasette_otel_file_exporter import _build_store

        monkeypatch.setenv("AWS_ENDPOINT_URL", "http://standard.test:9000")
        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://tigris.test:9000")
        store = _build_store(url="s3://bkt/pfx")
        # obstore reads AWS_ENDPOINT_URL itself at the client layer; .config
        # only reflects explicitly passed kwargs, so "no endpoint in config"
        # proves we did NOT pass the fallback kwarg over the standard var
        assert store.inner.config.get("endpoint") is None

    def test_no_vars_no_kwarg(self):
        from datasette_otel_file_exporter import _build_store

        store = _build_store(url="s3://bkt/pfx")
        assert store.inner.config.get("endpoint") is None


class TestUrlConfig:
    "url_config is obstore's per-store config; datasette resolves $env first."

    @pytest.fixture(autouse=True)
    def _clean_env(self, monkeypatch):
        for var in ("AWS_ENDPOINT_URL", "AWS_ENDPOINT_URL_S3"):
            monkeypatch.delenv(var, raising=False)

    def test_passed_to_obstore(self):
        from datasette_otel_file_exporter import _build_store

        store = _build_store(
            url="s3://bkt/pfx",
            url_config={
                "access_key_id": "cfg-key",
                "secret_access_key": "cfg-secret",
                "endpoint": "http://127.0.0.1:7070",
            },
        )
        assert store.inner.config["access_key_id"] == "cfg-key"
        assert store.inner.config["secret_access_key"] == "cfg-secret"
        assert store.inner.config["endpoint"] == "http://127.0.0.1:7070"
        assert store.inner.client_options is None

    def test_beats_tigris_fallback(self, monkeypatch):
        from datasette_otel_file_exporter import _build_store

        monkeypatch.setenv("AWS_ENDPOINT_URL_S3", "http://tigris.test:9000")
        store = _build_store(
            url="s3://bkt/pfx", url_config={"endpoint": "http://mine.test"}
        )
        assert store.inner.config["endpoint"] == "http://mine.test"

    def test_unset_env_keys_dropped(self, capsys):
        from datasette_otel_file_exporter import _resolve_url_config

        resolved = _resolve_url_config({"access_key_id": None, "region": "auto"})
        assert resolved == {"region": "auto"}
        assert "url_config.access_key_id is unset" in capsys.readouterr().err

    def test_must_be_mapping(self):
        from datasette_otel_file_exporter import _resolve_url_config

        with pytest.raises(ValueError, match="mapping"):
            _resolve_url_config("access_key_id=x")
