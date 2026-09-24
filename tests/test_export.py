"""
End-to-end: Datasette serving requests -> files read back with duckdb, the
interface users will actually use. Both formats; the default is ndjson.
"""

import glob
import gzip
import json
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import duckdb
import pytest
from conftest import flush
from datasette.app import Datasette

import datasette_otel_file_exporter
from datasette_otel_file_exporter.exporter import (
    FLUSH_SPAN,
    SCHEMA_METADATA_KEY,
    SCOPE_NAME,
    parquet_schema,
)

FORMATS = ["ndjson", "parquet"]
SUFFIX = {"ndjson": ".ndjson.gz", "parquet": ".parquet"}
READER = {"ndjson": "read_ndjson", "parquet": "read_parquet"}


def make_datasette(files=None, **plugin_settings):
    config = None
    if plugin_settings:
        config = {"plugins": {"datasette-otel-file-exporter": plugin_settings}}
    return Datasette(files or [], memory=not files, config=config)


def read(tel_dir, sql, format="ndjson"):
    table = f"{READER[format]}('{tel_dir}/traces/**/*{SUFFIX[format]}')"
    return duckdb.sql(sql.replace("$T", table)).fetchall()


def exported_files(tel_dir):
    return glob.glob(f"{tel_dir}/**/*.*", recursive=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("format", FORMATS)
async def test_end_to_end(tmp_path, demo_db, format):
    tel = tmp_path / "tel"
    datasette = make_datasette([str(demo_db)], path=str(tel), format=format)
    assert (await datasette.client.get("/")).status_code == 200
    assert (await datasette.client.get("/demo/plants")).status_code == 200
    flush()

    # Only this format's files exist
    assert all(f.endswith(SUFFIX[format]) for f in exported_files(tel))

    names = {row[0] for row in read(tel, "SELECT DISTINCT name FROM $T", format)}
    # The whole point of import-time wiring: the startup trace is captured
    assert "datasette.startup" in names
    assert any(name.startswith("GET ") for name in names)
    assert "db.query" in names

    routes = read(
        tel,
        "SELECT attributes ->> 'http.route' FROM $T WHERE name LIKE 'GET %'",
        format,
    )
    assert routes and all(route for (route,) in routes)

    queries = read(
        tel,
        "SELECT attributes ->> 'db.query.text' FROM $T "
        "WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL",
        format,
    )
    assert any("plants" in text for (text,) in queries)

    durations = read(tel, "SELECT duration_ns FROM $T", format)
    assert all(0 < ns < 60_000_000_000 for (ns,) in durations)


@pytest.mark.asyncio
async def test_default_format_is_ndjson(tmp_path):
    tel = tmp_path / "tel"
    datasette = make_datasette(path=str(tel))
    await datasette.client.get("/")
    flush()
    files = exported_files(tel)
    assert files and all(f.endswith(".ndjson.gz") for f in files)


@pytest.mark.asyncio
async def test_parquet_schema_contract(tmp_path):
    """The tripwire that makes schema changes deliberate.

    Column names and duckdb-visible types are the user-facing contract;
    the arrow schema and file metadata are asserted at the pyarrow level.
    """
    tel = tmp_path / "tel"
    datasette = make_datasette(path=str(tel), format="parquet")
    await datasette.client.get("/")
    flush()

    (file,) = glob.glob(f"{tel}/traces/*/*/*/*/*.parquet")

    described = duckdb.sql(f"DESCRIBE SELECT * FROM read_parquet('{file}')")
    assert [(name, dtype) for name, dtype, *_ in described.fetchall()] == [
        ("trace_id", "VARCHAR"),
        ("span_id", "VARCHAR"),
        ("parent_span_id", "VARCHAR"),
        ("name", "VARCHAR"),
        ("kind", "VARCHAR"),
        ("start_time", "TIMESTAMP WITH TIME ZONE"),
        ("start_time_unix_nano", "BIGINT"),
        ("end_time_unix_nano", "BIGINT"),
        ("duration_ns", "BIGINT"),
        ("status_code", "VARCHAR"),
        ("status_message", "VARCHAR"),
        ("service_name", "VARCHAR"),
        ("scope_name", "VARCHAR"),
        ("attributes", "VARCHAR"),
        ("resource", "VARCHAR"),
        ("events", "VARCHAR"),
        ("links", "VARCHAR"),
    ]

    import pyarrow.parquet as pq

    schema = parquet_schema()
    file_schema = pq.read_schema(file)
    assert file_schema.names == schema.names
    assert [str(t) for t in file_schema.types] == [str(t) for t in schema.types]
    assert file_schema.metadata[SCHEMA_METADATA_KEY.encode()] == b"1"

    # And through duckdb, the way a user would check what they have
    ((value,),) = duckdb.sql(
        f"SELECT value FROM parquet_kv_metadata('{file}') "
        f"WHERE key = '{SCHEMA_METADATA_KEY}'"
    ).fetchall()
    assert bytes(value) == b"1"


@pytest.mark.asyncio
async def test_ndjson_schema_contract(tmp_path):
    "Same tripwire for the ndjson encoding: keys, order, and duckdb's view."
    tel = tmp_path / "tel"
    datasette = make_datasette(path=str(tel))
    await datasette.client.get("/")
    flush()

    (file,) = glob.glob(f"{tel}/traces/*/*/*/*/*.ndjson.gz")
    with gzip.open(file, "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    assert rows
    for row in rows:
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
        assert isinstance(row["attributes"], dict)
        assert isinstance(row["resource"], dict)
        assert isinstance(row["events"], list)
        assert isinstance(row["links"], list)

    described = duckdb.sql(f"DESCRIBE SELECT * FROM read_ndjson('{file}')")
    types = {name: dtype for name, dtype, *_ in described.fetchall()}
    assert types["duration_ns"] == "BIGINT"
    assert types["attributes"].startswith("STRUCT(")
    assert types["resource"].startswith("STRUCT(")
    # Whether duckdb infers the ISO string as TIMESTAMP varies by version
    # (1.5.5 python: yes; the 1.5.1 CLI: VARCHAR), and a naive TIMESTAMP
    # cast to TIMESTAMPTZ picks up the session zone. start_time is for eyes
    # and jq; the cookbook does time math from the nanosecond column, which
    # reads identically in both formats
    ((bucketed,),) = duckdb.sql(
        f"""
        SELECT count(DISTINCT time_bucket(INTERVAL 1 minute,
                                          to_timestamp(start_time_unix_nano / 1e9)))
        FROM read_ndjson('{file}')
        """
    ).fetchall()
    assert bucketed >= 1


@pytest.mark.asyncio
async def test_dormant_without_path(tmp_path, capsys):
    datasette = make_datasette()
    assert (await datasette.client.get("/")).status_code == 200
    flush()

    assert datasette_otel_file_exporter._state["mode"] == "dormant"
    assert exported_files(tmp_path) == []
    err = capsys.readouterr().err
    assert err.count("no path or url configured") == 1

    # A second startup (another Datasette instance) does not log again
    another = make_datasette()
    await another.client.get("/")
    assert "no path or url configured" not in capsys.readouterr().err


@pytest.mark.asyncio
async def test_config_knobs_reach_the_exporter(tmp_path):
    datasette = make_datasette(
        path=str(tmp_path / "tel"),
        format="parquet",
        flush_interval_seconds=2,
        max_buffer_spans=77,
    )
    await datasette.client.get("/")

    state = datasette_otel_file_exporter._state
    delegate = state["exporter"]._delegate
    assert delegate._format == "parquet"
    assert delegate._flush_interval == 2.0
    assert delegate._max_buffer_spans == 77
    # BatchSpanProcessor cadence follows flush_interval so spans reach the
    # exporter at least that often
    assert state["processor"]._batch_processor._schedule_delay == 2.0


@pytest.mark.asyncio
async def test_unusable_path_does_not_break_serving(tmp_path, capsys):
    blocker = tmp_path / "blocker"
    blocker.write_text("i am a file, not a directory")
    datasette = make_datasette(path=str(blocker / "tel"))
    assert (await datasette.client.get("/")).status_code == 200
    flush()

    assert datasette_otel_file_exporter._state["mode"] == "dormant"
    assert "cannot open store" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_unknown_format_fails_startup(tmp_path):
    datasette = make_datasette(path=str(tmp_path / "tel"), format="csv")
    with pytest.raises(ValueError, match="unknown format 'csv'"):
        await datasette.client.get("/")


@pytest.mark.asyncio
async def test_parquet_without_pyarrow_fails_startup(tmp_path, monkeypatch):
    "A missing extra is a config error: loud at startup, not silent drops."
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    datasette = make_datasette(path=str(tmp_path / "tel"), format="parquet")
    with pytest.raises(ImportError, match=r"\[parquet\]"):
        await datasette.client.get("/")


@pytest.mark.asyncio
async def test_url_without_obstore_fails_startup(tmp_path, monkeypatch):
    monkeypatch.setitem(sys.modules, "obstore", None)
    datasette = make_datasette(url=f"file://{tmp_path}/tel")
    with pytest.raises(ImportError, match=r"\[obstore\]"):
        await datasette.client.get("/")


@pytest.mark.asyncio
async def test_url_via_obstore_from_url(tmp_path):
    "url: goes through obstore.store.from_url - no exporter changes."
    tel = tmp_path / "tel"
    tel.mkdir()
    datasette = make_datasette(url=f"file://{tel}")
    assert (await datasette.client.get("/")).status_code == 200
    flush()

    names = {row[0] for row in read(tel, "SELECT DISTINCT name FROM $T")}
    assert "datasette.startup" in names
    assert any(name.startswith("GET ") for name in names)


@pytest.mark.asyncio
async def test_path_and_url_mutually_exclusive(tmp_path):
    datasette = make_datasette(path=str(tmp_path / "a"), url=f"file://{tmp_path}/b")
    with pytest.raises(ValueError, match="mutually exclusive"):
        await datasette.client.get("/")


def _free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def test_sigint_flushes_the_tail(tmp_path, demo_db):
    "Killing the server after a request still yields that request's spans."
    tel = tmp_path / "tel"
    port = _free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "datasette",
            str(demo_db),
            "-p",
            str(port),
            "-s",
            "plugins.datasette-otel-file-exporter.path",
            str(tel),
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 20
        while True:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/demo/plants", timeout=1
                )
                break
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.2)
        process.send_signal(signal.SIGINT)
        process.wait(timeout=20)
    finally:
        process.kill()

    names = {row[0] for row in read(tel, "SELECT DISTINCT name FROM $T")}
    assert "datasette.startup" in names
    assert any(name.startswith("GET ") for name in names)


@pytest.mark.asyncio
async def test_base_install_needs_no_extras(tmp_path, monkeypatch):
    "path: + ndjson (the defaults) must work with neither pyarrow nor obstore."
    monkeypatch.setitem(sys.modules, "pyarrow", None)
    monkeypatch.setitem(sys.modules, "obstore", None)
    tel = tmp_path / "tel"
    datasette = make_datasette(path=str(tel))
    assert (await datasette.client.get("/")).status_code == 200
    flush()
    names = {row[0] for row in read(tel, "SELECT DISTINCT name FROM $T")}
    assert "datasette.startup" in names


@pytest.mark.asyncio
@pytest.mark.parametrize("format", FORMATS)
async def test_exporter_records_its_own_writes(tmp_path, demo_db, format):
    """Each file's write is described by an otel_file_exporter.flush span in
    the *next* file, with the actual on-disk size."""
    import os

    tel = tmp_path / "tel"
    datasette = make_datasette([str(demo_db)], path=str(tel), format=format)
    assert (await datasette.client.get("/")).status_code == 200
    flush()
    (first,) = exported_files(tel)
    # The flush span is queued, not yet written - and on its own it must not
    # roll a second file
    flush()
    assert exported_files(tel) == [first]

    assert (await datasette.client.get("/demo/plants")).status_code == 200
    flush()
    assert len(exported_files(tel)) == 2

    own = read(
        tel,
        "SELECT name, status_code, scope_name, "
        "attributes ->> 'otel_file_exporter.file.key', "
        "(attributes ->> 'otel_file_exporter.file.size_bytes')::BIGINT, "
        "(attributes ->> 'otel_file_exporter.spans')::BIGINT, "
        "attributes ->> 'otel_file_exporter.trigger', "
        "attributes ->> 'otel_file_exporter.store', "
        "attributes ->> 'otel_file_exporter.format' "
        f"FROM $T WHERE scope_name = '{SCOPE_NAME}'",
        format,
    )
    ((name, status, _scope, key, size, spans, trigger, store, fmt),) = own
    assert name == FLUSH_SPAN and status == "OK"
    assert str(tel / key) == first
    assert size == os.path.getsize(first)
    assert (trigger, store, fmt) == ("force_flush", "file", format)
    first_rows = read(
        tel,
        f"SELECT count(*) FROM {READER[format]}('{first}')",
        format,
    )[0][0]
    assert spans == first_rows


def test_idle_server_stops_writing(tmp_path, demo_db):
    """The feedback loop, end to end: after the last request is exported, a
    server with a short flush interval must not keep rolling files that
    describe only their predecessors."""
    tel = tmp_path / "tel"
    port = _free_port()
    process = subprocess.Popen(
        [
            sys.executable,
            "-m",
            "datasette",
            str(demo_db),
            "-p",
            str(port),
            "-s",
            "plugins.datasette-otel-file-exporter.path",
            str(tel),
            "-s",
            "plugins.datasette-otel-file-exporter.flush_interval_seconds",
            "0.5",
        ],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    try:
        deadline = time.time() + 20
        while True:
            try:
                urllib.request.urlopen(
                    f"http://127.0.0.1:{port}/demo/plants", timeout=1
                )
                break
            except Exception:
                if time.time() > deadline:
                    raise
                time.sleep(0.2)
        # Let the request's spans roll (BSP cadence 0.5s + flush interval 0.5s)
        deadline = time.time() + 10
        while not exported_files(tel) and time.time() < deadline:
            time.sleep(0.1)
        time.sleep(1.5)
        settled = exported_files(tel)
        assert settled
        # Six more flush intervals of idleness: no new files
        time.sleep(3)
        assert exported_files(tel) == settled
    finally:
        process.send_signal(signal.SIGINT)
        try:
            process.wait(timeout=20)
        finally:
            process.kill()


@pytest.mark.asyncio
async def test_url_config_resolves_env_secrets(tmp_path, monkeypatch):
    "datasette's {'$env': ...} reaches obstore as the variable's value."
    import datasette_otel_file_exporter as plugin

    captured = {}
    real_build_store = plugin._build_store

    def spy(**kwargs):
        captured.update(kwargs)
        return real_build_store(path=str(tmp_path / "tel"))

    monkeypatch.setattr(plugin, "_build_store", spy)
    monkeypatch.setenv("TEST_TEL_KEY", "from-env")
    monkeypatch.delenv("TEST_TEL_UNSET", raising=False)
    datasette = make_datasette(
        url="s3://bkt/pfx",
        url_config={
            "access_key_id": {"$env": "TEST_TEL_KEY"},
            "secret_access_key": {"$env": "TEST_TEL_UNSET"},
            "region": "auto",
        },
    )
    assert (await datasette.client.get("/")).status_code == 200
    assert captured["url_config"] == {"access_key_id": "from-env", "region": "auto"}


@pytest.mark.asyncio
async def test_url_config_without_url(tmp_path):
    datasette = make_datasette(
        path=str(tmp_path / "tel"), url_config={"region": "auto"}
    )
    with pytest.raises(ValueError, match="'url_config' needs 'url'"):
        await datasette.client.get("/")
