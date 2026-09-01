"""
ParquetSpanExporter: buffer finished spans, roll them into Parquet files
through an obstore store.

Schema v1 is the plugin's public contract - dashboards and canned queries are
written against it. One row per span, flat, no nesting; the open-ended parts
(attributes, resource, events, links) are JSON strings. Hex-string ids,
strings for kind/status_code, JSON-in-string over Arrow maps: all three trade
file size for query ergonomics, deliberately. Every file embeds
``datasette_otel_parquet_schema = "1"`` in its Parquet metadata; changes
within v1 are additive only.

Hard rule: this module never imports from ``datasette`` and never touches the
Datasette instance. Writing a file must not re-enter the instrumented write
path - that structural immunity to the self-storage feedback loop (see
datasette's demos/otel/self_storage) is a design constraint, not an accident.
"""

import json
import secrets
import sys
import threading
import time
from datetime import datetime, timezone

import obstore
import pyarrow as pa
import pyarrow.parquet as pq
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult
from opentelemetry.trace import format_span_id, format_trace_id

SCHEMA_VERSION = "1"

SCHEMA = pa.schema(
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
    metadata={"datasette_otel_parquet_schema": SCHEMA_VERSION},
)


def _json(value):
    return json.dumps(value, ensure_ascii=False, default=str)


def _attributes_dict(attributes):
    # OTel attribute values are str/bool/int/float or homogeneous sequences;
    # sequences arrive as tuples, which json serializes as arrays.
    return {key: value for key, value in (attributes or {}).items()}


def span_to_row(span):
    "Convert a ReadableSpan to a schema-v1 row dict."
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
        "start_time": span.start_time // 1000,  # int microseconds since epoch
        "start_time_unix_nano": span.start_time,
        "end_time_unix_nano": span.end_time,
        "duration_ns": span.end_time - span.start_time,
        "status_code": status.status_code.name,
        "status_message": status.description,
        "service_name": str(resource_attributes.get("service.name", "")),
        "scope_name": span.instrumentation_scope.name
        if span.instrumentation_scope
        else "",
        "attributes": _json(_attributes_dict(span.attributes)),
        "resource": _json(resource_attributes),
        "events": _json(events) if events else None,
        "links": _json(links) if links else None,
    }


class ParquetSpanExporter(SpanExporter):
    """Rolling writer: buffer rows, write one Parquet file per flush.

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
    rows once they come due. Writes therefore happen on the processor's
    thread or this flusher thread - never the event loop, never a request
    path.

    ``clock`` returns unix seconds and exists so tests can fake time; it is
    used for both the roll decision and the file's hour partition.
    """

    def __init__(
        self,
        store,
        prefix="",
        flush_interval_seconds=10,
        max_buffer_spans=10000,
        clock=time.time,
        background_flush=True,
    ):
        self._store = store
        self._prefix = prefix.strip("/") + "/" if prefix.strip("/") else ""
        self._flush_interval = float(flush_interval_seconds)
        self._max_buffer_spans = int(max_buffer_spans)
        self._clock = clock
        self._lock = threading.Lock()
        self._rows = []
        self._buffer_started_at = None
        self._logged_errors = set()
        self._stop = threading.Event()
        self._flusher = None
        if background_flush:
            self._flusher = threading.Thread(
                target=self._flush_loop,
                name="datasette-otel-parquet-flusher",
                daemon=True,
            )
            self._flusher.start()

    def export(self, spans):
        now = self._clock()
        with self._lock:
            if not self._rows:
                self._buffer_started_at = now
            self._rows.extend(span_to_row(span) for span in spans)
            due = len(self._rows) >= self._max_buffer_spans or (
                now - self._buffer_started_at >= self._flush_interval
            )
            rows = self._take_rows() if due else None
        if rows is None:
            return SpanExportResult.SUCCESS
        return self._write(rows, now)

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
                rows = self._take_rows() if due else None
            if rows:
                self._write(rows, now)

    def _flush(self):
        with self._lock:
            rows = self._take_rows()
        if not rows:
            return SpanExportResult.SUCCESS
        return self._write(rows, self._clock())

    def _take_rows(self):
        "Caller holds the lock."
        rows, self._rows = self._rows, []
        self._buffer_started_at = None
        return rows

    def _write(self, rows, now):
        try:
            table = pa.Table.from_pylist(rows, schema=SCHEMA)
            sink = pa.BufferOutputStream()
            pq.write_table(table, sink, compression="zstd")
            obstore.put(
                self._store, self._key(now), sink.getvalue().to_pybytes()
            )
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
                    f"datasette-otel-parquet: dropping batch of {len(rows)} "
                    f"spans, write failed: "
                    f"{type(exception).__name__}: {exception}",
                    file=sys.stderr,
                )
            return SpanExportResult.FAILURE

    def _key(self, now):
        moment = datetime.fromtimestamp(now, tz=timezone.utc)
        file_id = f"{int(now * 1000):013d}-{secrets.token_hex(4)}"
        return (
            f"{self._prefix}traces/{moment:%Y}/{moment:%m}/{moment:%d}/"
            f"{moment:%H}/{file_id}.parquet"
        )
