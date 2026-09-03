"""
Where the files go. Two stores, one interface: ``put(key, bytes)``.

- ``LocalDirectoryStore`` - a directory, stdlib only. The base install
  writes gzipped NDJSON to a local path with no compiled dependency at all.
- obstore, via ``open_url_store`` - any ``s3://``, ``gs://``, ``az://`` or
  ``file://`` URL, behind the ``[obstore]`` extra. obstore's store objects
  already expose ``put(path, bytes)``, so nothing wraps them.

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


def open_url_store(url):
    """obstore's from_url, with the plugin's retry policy and one env shim.

    Credentials are never plugin config (datasette.yaml gets committed to
    repos): obstore's native chain reads the standard env vars
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
