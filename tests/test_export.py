"""
End-to-end: Datasette serving requests -> Parquet files read back with duckdb,
the interface users will actually use.
"""

import glob
import signal
import socket
import subprocess
import sys
import time
import urllib.request

import duckdb
import pytest
from datasette.app import Datasette

import datasette_otel_parquet
from conftest import flush

from datasette_otel_parquet.exporter import SCHEMA


def make_datasette(files=None, **plugin_settings):
    config = None
    if plugin_settings:
        config = {"plugins": {"datasette-otel-parquet": plugin_settings}}
    return Datasette(files or [], memory=not files, config=config)


def read(tel_dir, sql):
    return duckdb.sql(
        sql.replace("$T", f"read_parquet('{tel_dir}/traces/**/*.parquet')")
    ).fetchall()


@pytest.mark.asyncio
async def test_end_to_end(tmp_path, demo_db):
    tel = tmp_path / "tel"
    datasette = make_datasette([str(demo_db)], path=str(tel))
    assert (await datasette.client.get("/")).status_code == 200
    assert (await datasette.client.get("/demo/plants")).status_code == 200
    flush()

    names = {row[0] for row in read(tel, "SELECT DISTINCT name FROM $T")}
    # The whole point of import-time wiring: the startup trace is captured
    assert "datasette.startup" in names
    assert any(name.startswith("GET ") for name in names)
    assert "db.query" in names

    routes = read(
        tel,
        "SELECT attributes ->> 'http.route' FROM $T WHERE name LIKE 'GET %'",
    )
    assert routes and all(route for (route,) in routes)

    queries = read(
        tel,
        "SELECT attributes ->> 'db.query.text' FROM $T "
        "WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL",
    )
    assert any("plants" in text for (text,) in queries)

    durations = read(tel, "SELECT duration_ns FROM $T")
    assert all(0 < ns < 60_000_000_000 for (ns,) in durations)


@pytest.mark.asyncio
async def test_schema_contract(tmp_path):
    """The tripwire that makes schema changes deliberate.

    Column names and duckdb-visible types are the user-facing contract;
    the arrow schema and file metadata are asserted at the pyarrow level.
    """
    tel = tmp_path / "tel"
    datasette = make_datasette(path=str(tel))
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

    file_schema = pq.read_schema(file)
    assert file_schema.names == SCHEMA.names
    assert [str(t) for t in file_schema.types] == [str(t) for t in SCHEMA.types]
    assert (
        file_schema.metadata[b"datasette_otel_parquet_schema"] == b"1"
    )

    # And through duckdb, the way a user would check what they have
    ((value,),) = duckdb.sql(
        f"SELECT value FROM parquet_kv_metadata('{file}') "
        "WHERE key = 'datasette_otel_parquet_schema'"
    ).fetchall()
    assert bytes(value) == b"1"


@pytest.mark.asyncio
async def test_dormant_without_path(tmp_path, capsys):
    datasette = make_datasette()
    assert (await datasette.client.get("/")).status_code == 200
    flush()

    assert datasette_otel_parquet._state["mode"] == "dormant"
    assert glob.glob(f"{tmp_path}/**/*.parquet", recursive=True) == []
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
        flush_interval_seconds=2,
        max_buffer_spans=77,
    )
    await datasette.client.get("/")

    state = datasette_otel_parquet._state
    delegate = state["exporter"]._delegate
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

    assert datasette_otel_parquet._state["mode"] == "dormant"
    assert "cannot open store" in capsys.readouterr().err


@pytest.mark.asyncio
async def test_url_via_obstore_from_url(tmp_path):
    "Ticket 07: url: goes through obstore.store.from_url - no exporter changes."
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
    datasette = make_datasette(
        path=str(tmp_path / "a"), url=f"file://{tmp_path}/b"
    )
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
            "plugins.datasette-otel-parquet.path",
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
