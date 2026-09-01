"""
Flush Datasette's OpenTelemetry spans to Parquet files.

Datasette core emits spans through the OpenTelemetry API but never installs a
TracerProvider, so without help every span is a NonRecordingSpan. Provider
wiring happens in two phases (the same measured design as
datasette-otel-otlp, copied, not imported):

1. At module import (plugins load before ``invoke_startup()``), get a
   ``BatchSpanProcessor`` wrapped around a lazy exporter attached to a
   provider. This must happen at import: a span started through the API's
   ProxyTracer before a provider exists is non-recording forever, and the
   ``datasette.startup`` span starts before any plugin hook runs.

2. In the ``startup()`` hook, the first place plugin config is readable,
   resolve ``path``/``flush_interval_seconds``/``max_buffer_spans``/
   ``service_name`` and point the lazy exporter at a real
   ``ParquetSpanExporter`` - or, with no ``path`` configured, drop
   everything and sample nothing from then on (dormant).

One deliberate difference from the otlp plugin: when a real SDK
``TracerProvider`` is already installed (the ``opentelemetry-instrument``
agent, or datasette-otel-otlp imported first), this plugin does not go
dormant - it attaches its processor to that provider with
``add_span_processor()`` and skips sampler/resource management entirely (the
owner's sampler and service.name apply). Parquet-alongside-OTLP is a
supported combo.

Precedence: explicit ``OTEL_*`` environment variables beat plugin config,
implemented by omission where possible.
"""

import os
import sys
import threading

from datasette import hookimpl
from opentelemetry import trace
from opentelemetry.attributes import BoundedAttributes
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import (
    BatchSpanProcessor,
    SpanExporter,
    SpanExportResult,
)
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF, DEFAULT_ON, Sampler

from .exporter import ParquetSpanExporter

PLUGIN_NAME = "datasette-otel-parquet"
DEFAULT_SERVICE_NAME = "datasette"
DEFAULT_FLUSH_INTERVAL_SECONDS = 10
DEFAULT_MAX_BUFFER_SPANS = 10000


class _LazySpanExporter(SpanExporter):
    """Stands in for the Parquet exporter until plugin config is readable.

    The BatchSpanProcessor queues finished spans and only calls export()
    seconds after startup, so normally configure() has already run by the
    first export. If an export does arrive earlier, the spans are held here
    (bounded) and forwarded on configure().
    """

    _PENDING_LIMIT = 4096

    def __init__(self):
        self._lock = threading.Lock()
        self._configured = False
        self._delegate = None
        self._pending = []

    def configure(self, delegate):
        "delegate=None means dormant: drop everything, now and from now on."
        with self._lock:
            previous = self._delegate
            self._delegate = delegate
            self._configured = True
            pending, self._pending = self._pending, []
        if previous is not None and previous is not delegate:
            previous.shutdown()
        if delegate is not None and pending:
            delegate.export(pending)

    def export(self, spans):
        with self._lock:
            if not self._configured:
                if len(self._pending) < self._PENDING_LIMIT:
                    self._pending.extend(spans)
                return SpanExportResult.SUCCESS
            delegate = self._delegate
        if delegate is None:
            return SpanExportResult.SUCCESS
        return delegate.export(spans)

    def shutdown(self):
        with self._lock:
            delegate = self._delegate
        if delegate is not None:
            delegate.shutdown()

    def force_flush(self, timeout_millis=30000):
        with self._lock:
            delegate = self._delegate
        if delegate is not None:
            return delegate.force_flush(timeout_millis)
        return True


class _DeferredSampler(Sampler):
    "ParentBased(ALWAYS_ON) until the startup hook swaps in the configured one."

    def __init__(self):
        self._delegate = DEFAULT_ON

    def set_delegate(self, sampler):
        self._delegate = sampler

    def should_sample(self, *args, **kwargs):
        return self._delegate.should_sample(*args, **kwargs)

    def get_description(self):
        return f"{PLUGIN_NAME}({self._delegate.get_description()})"


# Module state, rebuilt by _install(). "mode" is one of:
#   "pending"  - processor is wired (own or attached provider), waiting for
#                the startup hook
#   "active"   - exporting
#   "dormant"  - no path configured; recording is off (when we own the
#                provider) or the processor discards (when attached)
#   "inert"    - a non-SDK provider we cannot attach to; we do nothing
# "owns_provider" records which of install/attach happened.
_state = {}


def _log(message):
    print(f"{PLUGIN_NAME}: {message}", file=sys.stderr)


def _install():
    "Runs at module import; also re-runnable by tests after resetting otel globals."
    _state.clear()
    existing = trace.get_tracer_provider()

    if isinstance(existing, (trace.ProxyTracerProvider, trace.NoOpTracerProvider)):
        # No one owns tracing yet: install our own provider (two-phase).
        resource_attributes = {}
        if "OTEL_SERVICE_NAME" not in os.environ:
            resource_attributes["service.name"] = DEFAULT_SERVICE_NAME
        resource = Resource.create(resource_attributes)

        # If OTEL_TRACES_SAMPLER is set, let the SDK build the sampler from
        # the environment (env beats plugin config); otherwise install a
        # delegating sampler the startup hook can retarget.
        sampler = (
            None if "OTEL_TRACES_SAMPLER" in os.environ else _DeferredSampler()
        )

        exporter = _LazySpanExporter()
        if sampler is not None:
            provider = TracerProvider(sampler=sampler, resource=resource)
        else:
            provider = TracerProvider(resource=resource)
        processor = BatchSpanProcessor(exporter)
        provider.add_span_processor(processor)
        trace.set_tracer_provider(provider)

        _state.update(
            mode="pending",
            owns_provider=True,
            provider=provider,
            processor=processor,
            resource=resource,
            sampler=sampler,
            exporter=exporter,
            dormant_logged=False,
        )
        return

    if isinstance(existing, TracerProvider):
        # Someone else (the agent, or datasette-otel-otlp imported first)
        # owns the provider. Join it: their sampler and service.name apply,
        # our processor sees every span that ends from here on - including
        # the whole startup trace, which has not ended yet.
        exporter = _LazySpanExporter()
        processor = BatchSpanProcessor(exporter)
        existing.add_span_processor(processor)
        _log("attaching to the already-installed TracerProvider")
        _state.update(
            mode="pending",
            owns_provider=False,
            provider=existing,
            processor=processor,
            resource=None,
            sampler=None,
            exporter=exporter,
            dormant_logged=False,
        )
        return

    _log(
        "a non-SDK TracerProvider is installed; cannot attach a span "
        "processor - Parquet export is disabled"
    )
    _state["mode"] = "inert"


def _set_service_name(resource, service_name):
    # Resource is immutable by design, but every span holds this exact object
    # by reference - including the still-open datasette.startup span - so
    # replacing its attribute mapping retrofits the name onto everything not
    # yet exported.
    attributes = dict(resource.attributes)
    attributes["service.name"] = service_name
    resource._attributes = BoundedAttributes(attributes=attributes, immutable=True)


def _set_schedule_delay(processor, millis):
    """Best-effort: retune the BatchSpanProcessor's export cadence.

    The processor is built at import (default 5s cadence) but
    flush_interval_seconds is only readable at startup. The worker re-reads
    its delay every loop, so mutating it takes effect on the next wakeup.
    Internals differ across SDK versions; on failure the exporter still
    rolls correctly, just on the default 5s visit cadence.
    """
    try:
        batch = processor._batch_processor  # opentelemetry-sdk >= 1.34
        batch._schedule_delay_millis = millis
        batch._schedule_delay = millis / 1000
    except AttributeError:
        try:
            processor.schedule_delay_millis = millis  # older layouts
        except AttributeError:
            pass


def _build_store(path=None, url=None):
    """LocalStore for path:, from_url for url: - the same write path either way.

    Credentials for object-store URLs are never plugin config (datasette.yaml
    gets committed to repos): obstore's native chain reads the standard env
    vars (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL, ...),
    instance metadata, etc., with automatic refresh.
    """
    if url is not None:
        from datetime import timedelta

        from obstore.store import from_url

        # Spans are telemetry, not ledger entries: one retry (plus obstore's
        # ~30s per-request timeout), then the exporter drops the batch with
        # one log line. Never buffer unboundedly toward an unreachable
        # bucket, never block process exit long on one. Client options
        # (timeouts, allow_http) stay env-driven - passing client_options
        # here would override the environment wholesale.
        retry_config = {
            "max_retries": 1,
            "retry_timeout": timedelta(seconds=30),
            "backoff": {
                "init_backoff": timedelta(milliseconds=250),
                "max_backoff": timedelta(seconds=2),
                "base": 2,
            },
        }
        store_kwargs = {}
        # Fly.io's Tigris injects AWS_ENDPOINT_URL_S3, which obstore does not
        # read; a per-key config kwarg fills the gap without touching
        # client_options. AWS_ENDPOINT_URL, when set, wins by omission.
        if (
            url.startswith("s3://")
            and "AWS_ENDPOINT_URL" not in os.environ
            and os.environ.get("AWS_ENDPOINT_URL_S3")
        ):
            store_kwargs["endpoint"] = os.environ["AWS_ENDPOINT_URL_S3"]
        return from_url(url, retry_config=retry_config, **store_kwargs)
    from obstore.store import LocalStore

    return LocalStore(prefix=path, mkdir=True)


def _configure(config):
    "Second phase: called from the startup() hook with plugin config."
    if _state.get("mode") == "inert":
        return

    owns = _state["owns_provider"]
    if (
        owns
        and config.get("service_name")
        and "OTEL_SERVICE_NAME" not in os.environ
    ):
        _set_service_name(_state["resource"], str(config["service_name"]))

    path = config.get("path")
    url = config.get("url")
    if path and url:
        # Loud, not last-one-wins: refusing to guess where telemetry goes.
        raise ValueError(
            f"{PLUGIN_NAME}: 'path' and 'url' are mutually exclusive - "
            "configure exactly one"
        )
    if not path and not url:
        # Dormant: no destination anywhere. When we own the provider, stop
        # recording spans too, so an unconfigured install costs as close to
        # nothing as an installed SDK provider can. When attached, the
        # provider is someone else's - just discard quietly.
        if owns and _state["sampler"] is not None:
            _state["sampler"].set_delegate(ALWAYS_OFF)
        _state["exporter"].configure(None)
        if not _state["dormant_logged"]:
            _log("no path or url configured - Parquet export is disabled")
            _state["dormant_logged"] = True
        _state["mode"] = "dormant"
        return

    flush_interval = float(
        config.get("flush_interval_seconds", DEFAULT_FLUSH_INTERVAL_SECONDS)
    )
    max_buffer_spans = int(
        config.get("max_buffer_spans", DEFAULT_MAX_BUFFER_SPANS)
    )
    try:
        store = _build_store(
            path=str(path) if path else None,
            url=str(url) if url else None,
        )
    except Exception as exception:
        _log(
            f"cannot open store for {url or path!r} "
            f"({type(exception).__name__}: {exception}) - "
            "Parquet export is disabled"
        )
        if owns and _state["sampler"] is not None:
            _state["sampler"].set_delegate(ALWAYS_OFF)
        _state["exporter"].configure(None)
        _state["mode"] = "dormant"
        return
    _state["exporter"].configure(
        ParquetSpanExporter(
            store,
            flush_interval_seconds=flush_interval,
            max_buffer_spans=max_buffer_spans,
        )
    )
    # Visit the exporter at least once per flush interval, unless the
    # operator tuned the BSP cadence themselves.
    if "OTEL_BSP_SCHEDULE_DELAY" not in os.environ:
        _set_schedule_delay(_state["processor"], int(flush_interval * 1000))
    _state["mode"] = "active"

    if not owns:
        # Attached to a foreign provider: its sampler decides what we see.
        # datasette-otel-otlp installed with no endpoint sets ALWAYS_OFF at
        # its own startup - warn when we can see that has already happened.
        description = ""
        try:
            description = _state["provider"].sampler.get_description()
        except Exception:
            pass
        # Match ALWAYS_OFF as the effective decision ("AlwaysOffSampler",
        # possibly wrapped or as a ParentBased root) - NOT the default
        # ParentBased description, whose remoteParentNotSampled arms also
        # mention AlwaysOffSampler.
        if (
            description == "AlwaysOffSampler"
            or "(AlwaysOffSampler)" in description
            or "root:AlwaysOffSampler" in description
        ):
            _log(
                "configured, but the TracerProvider this plugin attached to "
                "samples nothing (sampler: %s) - no spans will be recorded. "
                "Is datasette-otel-otlp installed without an endpoint?"
                % description
            )


_install()


@hookimpl
def startup(datasette):
    _configure(datasette.plugin_config(PLUGIN_NAME) or {})
