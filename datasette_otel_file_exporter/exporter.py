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
"""

import gzip
import json
import secrets
import sys
import threading
import time
from datetime import datetime, timedelta, timezone

from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import format_span_id, format_trace_id

SCHEMA_VERSION = 1
SCHEMA_METADATA_KEY = "datasette_otel_file_exporter_schema"
DEFAULT_FORMAT = "ndjson"

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)


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
        raise ValueError(
            f"unknown format {name!r} - expected one of {sorted(FORMATS)}"
        )
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
    ):
        check_format(format)
        self._store = store
        self._format = format
        self._suffix, self._encode, _ = FORMATS[format]
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._flush_interval = float(flush_interval_seconds)
        self._max_buffer_spans = int(max_buffer_spans)
        self._clock = clock
        self._lock = threading.Lock()
        self._records = []
        self._buffer_started_at = None
        self._logged_errors = set()
        self._stop = threading.Event()
        self._flusher = None
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
            if not self._records:
                self._buffer_started_at = now
            self._records.extend(span_to_record(span) for span in spans)
            due = len(self._records) >= self._max_buffer_spans or (
                now - self._buffer_started_at >= self._flush_interval
            )
            records = self._take_records() if due else None
        if records is None:
            return SpanExportResult.SUCCESS
        return self._write(records, now)

    def force_flush(self, timeout_millis=30000):
        return self._flush() is not SpanExportResult.FAILURE

    def shutdown(self):
        self._stop.set()
        if self._flusher is not None:
            self._flusher.join(timeout=2)
        self._flush()

    def _flush_loop(self):
        poll = min(max(self._flush_interval / 4, 0.25), self._flush_interval)
        while not self._stop.wait(poll):
            now = self._clock()
            with self._lock:
                due = (
                    self._buffer_started_at is not None
                    and now - self._buffer_started_at >= self._flush_interval
                )
                records = self._take_records() if due else None
            if records:
                self._write(records, now)

    def _flush(self):
        with self._lock:
            records = self._take_records()
        if not records:
            return SpanExportResult.SUCCESS
        return self._write(records, self._clock())

    def _take_records(self):
        "Caller holds the lock."
        records, self._records = self._records, []
        self._buffer_started_at = None
        return records

    def _write(self, records, now):
        try:
            self._store.put(self._key(now), self._encode(records))
            # A recovery: the next outage deserves a fresh log line
            self._logged_errors.clear()
            return SpanExportResult.SUCCESS
        except Exception as exception:  # never take down the request path
            # Dedupe on the error class: messages embed per-file keys and
            # timings, so message-level dedupe would log every batch
            # (measured against a downed S3 gateway).
            if type(exception).__name__ not in self._logged_errors:
                self._logged_errors.add(type(exception).__name__)
                print(
                    f"datasette-otel-file-exporter: dropping batch of "
                    f"{len(records)} spans, write failed: "
                    f"{type(exception).__name__}: {exception}",
                    file=sys.stderr,
                )
            return SpanExportResult.FAILURE

    def _key(self, now):
        moment = datetime.fromtimestamp(now, tz=timezone.utc)
        file_id = f"{int(now * 1000):013d}-{secrets.token_hex(4)}"
        return (
            f"{self._prefix}traces/{moment:%Y}/{moment:%m}/{moment:%d}/"
            f"{moment:%H}/{file_id}.{self._suffix}"
        )
