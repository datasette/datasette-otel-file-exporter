# Demo flow - the point: two commands from checkout to queryable traces.
#
#   just demo        # terminal 1: serve demo.db, flushing Parquet every 2s
#   ...browse http://localhost:8002 a bit...
#   just query       # terminal 2: the canned DuckDB queries below
#
# Everything runs against an editable checkout of ~/projects/datasette on the
# phase-1 otel branch (asg017/otel-phase1-* or later) - no released datasette
# emits these spans yet. See PLAN.md.

telemetry := "./telemetry"
glob := telemetry / "traces/**/*.parquet"

default:
    @just --list --unsorted

# Run the test suite
#
# --no-project matters: the project venv resolves `datasette` from PyPI, which
# shadows the --with-editable checkout on sys.path - and PyPI's alpha does not
# emit the spans this plugin exports. datasette-otel-otlp is included for the
# coexistence tests (they skip without it).
test *options:
    uv run --no-project --isolated \
      --with-editable . \
      --with-editable ~/projects/datasette \
      --with-editable ~/work/simonw/datasette-otel-otlp \
      --with pytest --with pytest-asyncio --with duckdb \
      pytest {{ options }}

# Generate demo.db (200-row table) if missing
demo-db:
    @[ -e demo.db ] || sqlite3 demo.db "create table plants(id integer primary key, name text, height_cm real); with recursive n(i) as (select 1 union all select i + 1 from n where i < 200) insert into plants select i, 'plant ' || i, abs(random() % 300) from n;"

# Datasette writing Parquet to ./telemetry - one -s flag, no env vars
demo *options: demo-db
    uv run --no-project --isolated \
      --with-editable . \
      --with-editable ~/projects/datasette \
      datasette demo.db \
        -s plugins.datasette-otel-parquet.path {{ telemetry }} \
        -s plugins.datasette-otel-parquet.flush_interval_seconds 2 \
        -p 8002 {{ options }}

# Make a traced request against `just demo`
request path="/demo/plants":
    curl -s -o /dev/null -w "%{http_code}\n" 'http://localhost:8002{{ path }}'

# All three canned queries in one go
query: slowest sql-hotspots requests-per-minute

# The 20 slowest spans
slowest:
    duckdb -c "SELECT name, round(duration_ns / 1e6, 2) AS ms, trace_id FROM read_parquet('{{ glob }}') ORDER BY ms DESC LIMIT 20;"

# SQL statements ranked by total time spent in them
sql-hotspots:
    duckdb -c "SELECT left(regexp_replace(attributes ->> 'db.query.text', '\s+', ' ', 'g'), 72) AS sql, count(*) AS calls, round(sum(duration_ns) / 1e6, 2) AS total_ms FROM read_parquet('{{ glob }}') WHERE name = 'db.query' AND (attributes ->> 'db.query.text') IS NOT NULL GROUP BY sql ORDER BY total_ms DESC LIMIT 15;"

# Requests per minute
requests-per-minute:
    duckdb -c "SELECT time_bucket(INTERVAL 1 minute, start_time) AS minute, count(*) AS requests FROM read_parquet('{{ glob }}') WHERE name LIKE 'GET %' GROUP BY minute ORDER BY minute;"

# Recent traces, newest first - feed one id to `just trace`
traces:
    duckdb -c "SELECT trace_id, min(start_time) AS started, count(*) AS spans, any_value(name ORDER BY start_time_unix_nano) AS root FROM read_parquet('{{ glob }}') GROUP BY trace_id ORDER BY started DESC LIMIT 20;"

# One trace's spans, indented by depth
trace trace_id:
    duckdb -c "WITH RECURSIVE spans AS (SELECT * FROM read_parquet('{{ glob }}') WHERE trace_id = '{{ trace_id }}'), tree AS (SELECT span_id, name, start_time_unix_nano, duration_ns, 0 AS depth FROM spans WHERE parent_span_id IS NULL OR parent_span_id NOT IN (SELECT span_id FROM spans) UNION ALL SELECT s.span_id, s.name, s.start_time_unix_nano, s.duration_ns, t.depth + 1 FROM spans s JOIN tree t ON s.parent_span_id = t.span_id) SELECT repeat('· ', depth) || name AS span, round(duration_ns / 1e6, 3) AS ms FROM tree ORDER BY start_time_unix_nano;"

# Delete the demo's telemetry output
clean:
    rm -rf {{ telemetry }}
