"""
Where the files go. Two stores, one interface: ``put(key, bytes)``.

- ``LocalDirectoryStore`` - a directory, stdlib only. The base install
  writes gzipped NDJSON to a local path with no compiled dependency at all.
- ``UrlStore``, via ``open_url_store`` - any ``s3://``, ``gs://``, ``az://``
  or ``file://`` URL, behind the ``[obstore]`` extra. A thin wrapper over
  obstore's store object that pins every put to a single request.

Both expose ``scheme`` (``file`` for the directory, the URL's scheme
otherwise), which the exporter records on its own flush span.

Same hard rule as the exporter: nothing here imports from ``datasette``.
"""

import os
import secrets


class LocalDirectoryStore:
    """Write files under a root directory, creating partitions as needed.

    Each put lands in a temporary sibling and is renamed into place, so a
    reader globbing the directory (DuckDB, ``tail -f``, the demo) never
    sees a half-written file. The temp name never matches a format's glob.
    """

    scheme = "file"

    def __init__(self, root):
        self.root = os.path.abspath(str(root))
        os.makedirs(self.root, exist_ok=True)

    def put(self, key, data):
        path = os.path.join(self.root, *key.split("/"))
        os.makedirs(os.path.dirname(path), exist_ok=True)
        temporary = f"{path}.{secrets.token_hex(4)}.tmp"
        try:
            with open(temporary, "wb") as handle:
                handle.write(data)
            os.replace(temporary, path)
        except BaseException:
            try:
                os.unlink(temporary)
            except OSError:
                pass
            raise


class ObstoreUnavailable(ImportError):
    "A url: was configured but obstore is not installed."


def check_obstore():
    try:
        import obstore  # noqa: F401
    except ImportError as exception:
        raise ObstoreUnavailable(
            "'url' needs obstore, which is not installed: "
            "pip install 'datasette-otel-file-exporter[obstore]'"
        ) from exception


class UrlStore:
    """An obstore store behind ``put(key, bytes)``: one request per file.

    ``use_multipart=False`` makes the request count a guarantee rather than a
    measurement - obstore is Rust and exposes no per-call request or retry
    count, so this is how "how many PUTs did I pay for?" becomes answerable:
    it is the number of successful ``otel_file_exporter.flush`` spans (plus
    at most one retry each, per the retry policy below). Files are bounded by
    ``max_buffer_spans`` at single-digit megabytes, far under the 5 GB
    single-PUT limit, and skipping multipart also rules out the orphaned
    multipart uploads obstore's docs warn about.
    """

    def __init__(self, url, store):
        self.url = url
        self.scheme = url.split("://", 1)[0] if "://" in url else "unknown"
        self.inner = store  # the obstore store object

    def put(self, key, data):
        self.inner.put(key, data, use_multipart=False)


def open_url_store(url, config=None):
    """obstore's from_url, with the plugin's retry policy and one env shim.

    ``config`` is obstore's per-store configuration (``access_key_id``,
    ``secret_access_key``, ``endpoint``, ``region``, ...), from the plugin's
    ``url_config``; each key it sets beats the matching env var. Secrets
    reach it through datasette's ``{"$env": ...}`` / ``{"$file": ...}``
    resolution, so datasette.yaml never has to hold one. Everything it
    leaves out comes from obstore's native chain: the standard env vars
    (AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_ENDPOINT_URL, ...),
    instance metadata, etc., with automatic refresh.
    """
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
    store_config = {}
    # Fly.io's Tigris injects AWS_ENDPOINT_URL_S3, which obstore does not
    # read; a per-key config entry fills the gap without touching
    # client_options. AWS_ENDPOINT_URL, when set, wins by omission; an
    # explicit url_config endpoint wins by overwriting.
    if (
        url.startswith("s3://")
        and "AWS_ENDPOINT_URL" not in os.environ
        and os.environ.get("AWS_ENDPOINT_URL_S3")
    ):
        store_config["endpoint"] = os.environ["AWS_ENDPOINT_URL_S3"]
    store_config.update(config or {})
    return UrlStore(
        url, from_url(url, retry_config=retry_config, config=store_config or None)
    )
