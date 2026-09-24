"""
FileSpanExporter: buffer finished spans, roll them into files through any
store with a ``put(key, bytes)`` method - obstore for buckets, the plugin's
own stdlib writer for local directories.

Schema v1 is the plugin's public contract - dashboards and canned queries are
written against it. One record per span, flat at the top level; the
open-ended parts (attributes, resource, events, links) are nested. Hex-string
ids and string names for kind/status_code trade a few bytes for query
ergonomics, deliberately. The record has two encodings:

- ``ndjson`` (default, stdlib only): one JSON object per line, gzipped.
  Nested parts are real objects, ``start_time`` is an ISO-8601 string, and
  every line carries ``schema_version`` because a JSON file has nowhere else
  to put it. DuckDB's ``read_ndjson`` infers the nested parts as structs and
  ``attributes ->> 'key'`` works on them unchanged; jq gets ``.attributes``
  directly.
- ``parquet`` (needs the ``[parquet]`` extra): the nested parts become JSON
  strings, ``start_time`` is ``timestamp[us, UTC]``, and the schema version
  lives in the file's key-value metadata.

Changes within v1 are additive only, in both encodings.

Hard rule: this module never imports from ``datasette`` and never touches the
Datasette instance. Writing a file must not re-enter the instrumented write
path - that structural immunity to the self-storage feedback loop (see
datasette's demos/otel/self_storage) is a design constraint, not an accident.

The exporter does emit one span of its own per file written
(``otel_file_exporter.flush``: bytes, span count, trigger, store, outcome), so
the files describe the pipeline that wrote them. That span is the one
deliberate re-entry, and it is a feedback loop in miniature: a span about a
write is itself written, whose write emits a span. Nothing in the SDK breaks
the cycle - ``BatchSpanProcessor.on_end`` checks only the sampled flag, and
the SDK ``Tracer`` ignores ``_SUPPRESS_INSTRUMENTATION_KEY`` (verified,
opentelemetry-sdk 1.44). The exporter breaks it with one rule: **a buffer
holding only the exporter's own spans never rolls**. Own spans (recognised by
instrumentation scope) do not start the interval clock and do not trigger
``force_flush``; they ride along in the next real file. ``shutdown()`` does
write them, so the last real file's record survives the process. A failed
write drops its batch whole, own records included, so an outage leaves
exactly one own span buffered - the record of the most recent failed
attempt, which lands in the first file written after recovery.
"""

import gzip
import importlib.metadata
import json
import secrets
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from opentelemetry import metrics, trace
from opentelemetry.context import Context
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import (
    SpanKind,
    Status,
    StatusCode,
    format_span_id,
    format_trace_id,
)

SCHEMA_VERSION = 1
SCHEMA_METADATA_KEY = "datasette_otel_file_exporter_schema"
DEFAULT_FORMAT = "ndjson"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

# --- the exporter's own telemetry -------------------------------------------
#
# Plain strings here so this module stays datasette-free; registry.py wraps
# them in datasette's Attribute/SpanName/MetricName for documentation and the
# conformance tests. Names follow datasette's plugin-telemetry rules: the
# scope is the import package, signals live under a prefix the plugin owns
# (a short product name rather than the full package name, so that
# ``attributes ->> 'otel_file_exporter.file.size_bytes'`` stays typeable).

SCOPE_NAME = "datasette_otel_file_exporter"

FLUSH_SPAN = "otel_file_exporter.flush"
ATTR_TRIGGER = "otel_file_exporter.trigger"
ATTR_FORMAT = "otel_file_exporter.format"
ATTR_STORE = "otel_file_exporter.store"
ATTR_FILE_KEY = "otel_file_exporter.file.key"
ATTR_FILE_SIZE = "otel_file_exporter.file.size_bytes"
ATTR_SPANS = "otel_file_exporter.spans"
ATTR_ENCODE_NS = "otel_file_exporter.encode_ns"
ATTR_ERROR_TYPE = "error.type"  # semconv spelling, shared with datasette core

# Why a file rolled. A closed vocabulary - safe as a metric dimension.
TRIGGER_INTERVAL = "interval"
TRIGGER_MAX_SPANS = "max_spans"
TRIGGER_FORCE_FLUSH = "force_flush"
TRIGGER_SHUTDOWN = "shutdown"
TRIGGERS = (
    TRIGGER_INTERVAL,
    TRIGGER_MAX_SPANS,
    TRIGGER_FORCE_FLUSH,
    TRIGGER_SHUTDOWN,
)

METRIC_FILE_SIZE = "otel_file_exporter.file.size"  # histogram, By
METRIC_FILES = "otel_file_exporter.files"  # counter, {file}
# Bytes per file: gzipped NDJSON of a few spans is ~1 KB; a full
# max_buffer_spans batch is single-digit MB. Decades, so every size lands in
# a distinguishable bucket.
FILE_SIZE_BUCKETS = (1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000)


def package_version():
    try:
        return importlib.metadata.version("datasette-otel-file-exporter")
    except importlib.metadata.PackageNotFoundError:
        return "0"


def _json(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def _attributes_dict(attributes):
    # OTel attribute values are str/bool/int/float or homogeneous sequences;
    # sequences arrive as tuples, which json serializes as arrays.
    return {key: value for key, value in (attributes or {}).items()}


def span_to_record(span):
    "Convert a ReadableSpan to a schema-v1 record: flat top level, nested extras."
    parent = span.parent
    status = span.status
    events = [
        {
            "name": event.name,
            "timestamp_unix_nano": event.timestamp,
            "attributes": _attributes_dict(event.attributes),
        }
        for event in (span.events or ())
    ]
    links = [
        {
            "trace_id": format_trace_id(link.context.trace_id),
            "span_id": format_span_id(link.context.span_id),
            "attributes": _attributes_dict(link.attributes),
        }
        for link in (span.links or ())
    ]
    resource_attributes = _attributes_dict(span.resource.attributes)
    return {
        "trace_id": format_trace_id(span.context.trace_id),
        "span_id": format_span_id(span.context.span_id),
        "parent_span_id": format_span_id(parent.span_id) if parent else None,
        "name": span.name,
        "kind": span.kind.name,
        "start_time_unix_nano": span.start_time,
        "end_time_unix_nano": span.end_time,
        "duration_ns": span.end_time - span.start_time,
        "status_code": status.status_code.name,
        "status_message": status.description,
        "service_name": str(resource_attributes.get("service.name", "")),
        "scope_name": span.instrumentation_scope.name
        if span.instrumentation_scope
        else "",
        "attributes": _attributes_dict(span.attributes),
        "resource": resource_attributes,
        "events": events,
        "links": links,
    }


# --- ndjson -----------------------------------------------------------------


def _iso_utc(unix_nano):
    # timedelta arithmetic keeps microsecond precision exactly; going through
    # a float timestamp would not.
    moment = _EPOCH + timedelta(microseconds=unix_nano // 1000)
    return moment.strftime("%Y-%m-%dT%H:%M:%S.%fZ")


def ndjson_row(record):
    "The wire shape of one NDJSON line, as a dict (key order is the contract)."
    row = {"schema_version": SCHEMA_VERSION}
    for key, value in record.items():
        if key == "start_time_unix_nano":
            row["start_time"] = _iso_utc(value)
        row[key] = value
    return row


def encode_ndjson(records):
    text = "".join(_json(ndjson_row(record)) + "\n" for record in records)
    # mtime=0: identical input -> identical bytes; level 6 is the usual
    # speed/size knee, and these writes share a thread with span batching.
    return gzip.compress(text.encode("utf-8"), compresslevel=6, mtime=0)


# --- parquet ----------------------------------------------------------------


def parquet_schema():
    import pyarrow as pa

    return pa.schema(
        [
            pa.field("trace_id", pa.string(), nullable=False),
            pa.field("span_id", pa.string(), nullable=False),
            pa.field("parent_span_id", pa.string(), nullable=True),
            pa.field("name", pa.string(), nullable=False),
            pa.field("kind", pa.string(), nullable=False),
            pa.field("start_time", pa.timestamp("us", tz="UTC"), nullable=False),
            pa.field("start_time_unix_nano", pa.int64(), nullable=False),
            pa.field("end_time_unix_nano", pa.int64(), nullable=False),
            pa.field("duration_ns", pa.int64(), nullable=False),
            pa.field("status_code", pa.string(), nullable=False),
            pa.field("status_message", pa.string(), nullable=True),
            pa.field("service_name", pa.string(), nullable=False),
            pa.field("scope_name", pa.string(), nullable=False),
            pa.field("attributes", pa.string(), nullable=False),
            pa.field("resource", pa.string(), nullable=False),
            pa.field("events", pa.string(), nullable=True),
            pa.field("links", pa.string(), nullable=True),
        ],
        metadata={SCHEMA_METADATA_KEY: str(SCHEMA_VERSION)},
    )


def parquet_row(record):
    "The Parquet shape of one record: nested parts as JSON strings."
    row = dict(record)
    row["start_time"] = record["start_time_unix_nano"] // 1000  # int µs
    row["attributes"] = _json(record["attributes"])
    row["resource"] = _json(record["resource"])
    row["events"] = _json(record["events"]) if record["events"] else None
    row["links"] = _json(record["links"]) if record["links"] else None
    return row


def encode_parquet(records):
    import pyarrow as pa
    import pyarrow.parquet as pq

    table = pa.Table.from_pylist(
        [parquet_row(record) for record in records], schema=parquet_schema()
    )
    sink = pa.BufferOutputStream()
    pq.write_table(table, sink, compression="zstd")
    return sink.getvalue().to_pybytes()


# --- formats ----------------------------------------------------------------

# name -> (file suffix, encoder, module the encoder needs or None)
FORMATS = {
    "ndjson": ("ndjson.gz", encode_ndjson, None),
    "parquet": ("parquet", encode_parquet, "pyarrow"),
}


class FormatUnavailable(ImportError):
    "A known format whose optional dependency is not installed."


def check_format(name):
    """Validate a format name and its dependency; raise loudly, not later.

    Called at startup so a misconfigured instance fails to start with an
    actionable message, rather than dropping every batch with a log line.
    """
    if name not in FORMATS:
        raise ValueError(f"unknown format {name!r} - expected one of {sorted(FORMATS)}")
    module = FORMATS[name][2]
    if module is not None:
        try:
            __import__(module)
        except ImportError as exception:
            raise FormatUnavailable(
                f"format {name!r} needs {module}, which is not installed: "
                f"pip install 'datasette-otel-file-exporter[{name}]'"
            ) from exception


class FileSpanExporter(SpanExporter):
    """Rolling writer: buffer records, write one file per flush.

    export() runs on the BatchSpanProcessor's background thread; blocking it
    with a write is fine - that is exactly what the thread is for. A file is
    written when the oldest buffered span is ``flush_interval_seconds`` old,
    when ``max_buffer_spans`` accumulate, or on force_flush()/shutdown().
    Files are immutable once written.

    The interval trigger cannot live on the BatchSpanProcessor alone: its
    worker never calls export() while its queue is empty (measured,
    opentelemetry-sdk 1.44), so a burst of traffic followed by idle would
    leave the tail buffered here until the next request or process exit.
    ``background_flush`` starts a small daemon thread that flushes buffered
    records once they come due. Writes therefore happen on the processor's
    thread or this flusher thread - never the event loop, never a request
    path.

    ``clock`` returns unix seconds and exists so tests can fake time; it is
    used for both the roll decision and the file's hour partition.

    Every write is recorded as an ``otel_file_exporter.flush`` span through
    ``tracer`` (default: the global provider's tracer for this plugin's
    scope), plus a size histogram and a file counter through ``meter``
    (default: the global meter - a no-op unless an operator installs a
    MeterProvider). See the module docstring for how the exporter keeps its
    own spans from rolling files of their own.
    """

    def __init__(
        self,
        store,
        format=DEFAULT_FORMAT,
        prefix="",
        flush_interval_seconds=10,
        max_buffer_spans=10000,
        clock=time.time,
        background_flush=True,
        tracer=None,
        meter=None,
    ):
        check_format(format)
        self._store = store
        self._store_scheme = str(getattr(store, "scheme", "unknown"))
        self._format = format
        self._suffix, self._encode, _ = FORMATS[format]
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._flush_interval = float(flush_interval_seconds)
        self._max_buffer_spans = int(max_buffer_spans)
        self._clock = clock
        self._lock = threading.Lock()
        self._records = []
        # Records from other scopes - the ones that start the interval clock
        # and the ones force_flush() writes for. Caller holds the lock.
        self._foreign_count = 0
        self._buffer_started_at = None
        self._logged_errors = set()
        self._stop = threading.Event()
        self._flusher = None

        version = package_version()
        self._tracer = tracer or trace.get_tracer(SCOPE_NAME, version)
        meter = meter or metrics.get_meter(SCOPE_NAME, version)
        self._file_size = meter.create_histogram(
            METRIC_FILE_SIZE,
            unit="By",
            description="Encoded size of each telemetry file written",
            explicit_bucket_boundaries_advisory=FILE_SIZE_BUCKETS,
        )
        self._files = meter.create_counter(
            METRIC_FILES,
            unit="{file}",
            description=(
                "Telemetry files written, or attempted - error.type is set "
                "on the failures"
            ),
        )
        if background_flush:
            self._flusher = threading.Thread(
                target=self._flush_loop,
                name="datasette-otel-file-exporter-flusher",
                daemon=True,
            )
            self._flusher.start()

    def export(self, spans):
        now = self._clock()
        with self._lock:
            for span in spans:
                record = span_to_record(span)
                if record["scope_name"] != SCOPE_NAME:
                    if self._foreign_count == 0:
                        self._buffer_started_at = now
                    self._foreign_count += 1
                self._records.append(record)
            trigger = self._due(now)
            records = self._take_records() if trigger else None
        if records is None:
            return SpanExportResult.SUCCESS
        return self._write(records, now, trigger)

    def force_flush(self, timeout_millis=30000):
        return self._flush(TRIGGER_FORCE_FLUSH) is not SpanExportResult.FAILURE

    def shutdown(self):
        self._stop.set()
        if self._flusher is not None:
            self._flusher.join(timeout=2)
        # The one time a self-only buffer is written: the record of the last
        # real file would otherwise be lost with the process.
        self._flush(TRIGGER_SHUTDOWN, own_spans_too=True)

    def _flush_loop(self):
        poll = min(max(self._flush_interval / 4, 0.25), self._flush_interval)
        while not self._stop.wait(poll):
            now = self._clock()
            with self._lock:
                trigger = self._due(now)
                records = self._take_records() if trigger else None
            if records:
                self._write(records, now, trigger)

    def _due(self, now):
        "Which trigger fires for the current buffer, or None. Caller holds the lock."
        if len(self._records) >= self._max_buffer_spans:
            return TRIGGER_MAX_SPANS
        if (
            self._foreign_count
            and now - self._buffer_started_at >= self._flush_interval
        ):
            return TRIGGER_INTERVAL
        return None

    def _flush(self, trigger, own_spans_too=False):
        with self._lock:
            if not self._records or not (self._foreign_count or own_spans_too):
                return SpanExportResult.SUCCESS
            records = self._take_records()
        return self._write(records, self._clock(), trigger)

    def _take_records(self):
        "Caller holds the lock."
        records, self._records = self._records, []
        self._foreign_count = 0
        self._buffer_started_at = None
        return records

    def _write(self, records, now, trigger):
        key = self._key(now)
        dimensions = {ATTR_FORMAT: self._format, ATTR_STORE: self._store_scheme}
        # An explicit empty Context: this is a root span by design, whatever
        # ambient context the calling thread happens to carry.
        span = self._tracer.start_span(
            FLUSH_SPAN,
            context=Context(),
            kind=SpanKind.INTERNAL,
            attributes={
                **dimensions,
                ATTR_TRIGGER: trigger,
                ATTR_FILE_KEY: key,
                ATTR_SPANS: len(records),
            },
        )
        try:
            started = time.perf_counter_ns()
            data = self._encode(records)
            span.set_attribute(ATTR_ENCODE_NS, time.perf_counter_ns() - started)
            span.set_attribute(ATTR_FILE_SIZE, len(data))
            self._store.put(key, data)
            span.set_status(Status(StatusCode.OK))
            self._file_size.record(len(data), dimensions)
            self._files.add(1, dimensions)
            # A recovery: the next outage deserves a fresh log line
            self._logged_errors.clear()
            return SpanExportResult.SUCCESS
        except Exception as exception:  # never take down the request path
            error_type = type(exception).__name__
            span.set_attribute(ATTR_ERROR_TYPE, error_type)
            span.set_status(Status(StatusCode.ERROR, str(exception)))
            self._files.add(1, {**dimensions, ATTR_ERROR_TYPE: error_type})
            # Dedupe on the error class: messages embed per-file keys and
            # timings, so message-level dedupe would log every batch
            # (measured against a downed S3 gateway).
            if error_type not in self._logged_errors:
                self._logged_errors.add(error_type)
                print(
                    f"datasette-otel-file-exporter: dropping batch of "
                    f"{len(records)} spans, write failed: "
                    f"{error_type}: {exception}",
                    file=sys.stderr,
                )
            return SpanExportResult.FAILURE
        finally:
            span.end()

    def _key(self, now):
        moment = datetime.fromtimestamp(now, tz=timezone.utc)
        file_id = f"{int(now * 1000):013d}-{secrets.token_hex(4)}"
        return (
            f"{self._prefix}traces/{moment:%Y}/{moment:%m}/{moment:%d}/"
            f"{moment:%H}/{file_id}.{self._suffix}"
        )
