# Demo flow - the point: two commands from checkout to queryable traces.
#
#   just demo        # terminal 1: serve demo.db, flushing Parquet every 2s
#   ...browse http://localhost:8002 a bit...
#   just query       # terminal 2: the canned DuckDB queries below
#
# Everything runs against datasette's phase-1 otel branch, resolved through the
# [tool.uv.sources] override in pyproject.toml - no released datasette emits
# these spans yet. See NOTES.md.

telemetry := "./telemetry"
glob := telemetry / "traces/**/*.parquet"

# The sibling plugin the coexistence tests exercise
otlp_source := "datasette-otel-otlp @ git+https://github.com/datasette/datasette-otel-otlp"

# The S3 demo: versitygw serves a real S3 API over ./s3root; test creds only.
# Absolute: versitygw resolves its backend root after changing directory
s3_root := justfile_directory() / "s3root"
s3_bucket := "telemetry-demo"
s3_access := "testkey"
s3_secret := "testsecret"
s3_endpoint := "http://127.0.0.1:7070"

default:
    @just --list --unsorted

# Run the test suite (coexistence tests skip without datasette-otel-otlp)
test *options:
    uv run pytest {{ options }}

# Run the test suite with datasette-otel-otlp installed, so the coexistence
# tests run instead of skipping
test-coexistence *options:
    uv run --with "{{ otlp_source }}" pytest {{ options }}

# Generate demo.db (200-row table) if missing
demo-db:
    @[ -e demo.db ] || sqlite3 demo.db "create table plants(id integer primary key, name text, height_cm real); with recursive n(i) as (select 1 union all select i + 1 from n where i < 200) insert into plants select i, 'plant ' || i, abs(random() % 300) from n;"

# Datasette writing Parquet to ./telemetry - one -s flag, no env vars
demo *options: demo-db
    uv run datasette demo.db \
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

# --- The off-box story: same plugin, url: instead of path:, creds from env ---
#
#   just s3-gateway  +  just demo-s3  ->  browse  ->  just query-s3
#
# versitygw (https://github.com/versity/versitygw) is a real S3 server over a
# local directory - closer to production than a mock, no Docker.

# A live S3 API over {{ s3_root }} - terminal 0
s3-gateway:
    @command -v versitygw >/dev/null || { echo "No versitygw on PATH. brew install versity/versitygw/versitygw or grab a release from https://github.com/versity/versitygw/releases"; exit 1; }
    mkdir -p {{ s3_root }}/{{ s3_bucket }}
    versitygw -a {{ s3_access }} -s {{ s3_secret }} posix {{ s3_root }}

# Datasette shipping spans straight to the bucket - no collector, no backend
demo-s3 *options: demo-db
    AWS_ACCESS_KEY_ID={{ s3_access }} AWS_SECRET_ACCESS_KEY={{ s3_secret }} \
    AWS_ENDPOINT_URL={{ s3_endpoint }} AWS_REGION=us-east-1 AWS_ALLOW_HTTP=true \
    uv run datasette demo.db \
        -s plugins.datasette-otel-parquet.url s3://{{ s3_bucket }}/tel \
        -s plugins.datasette-otel-parquet.flush_interval_seconds 2 \
        -p 8002 {{ options }}

# DuckDB reads the bucket remotely - "a Datasette with a bucket has
# observability with no other service"
query-s3:
    duckdb -c "CREATE SECRET vgw (TYPE s3, KEY_ID '{{ s3_access }}', SECRET '{{ s3_secret }}', ENDPOINT '127.0.0.1:7070', USE_SSL false, URL_STYLE path, REGION 'us-east-1'); SELECT name, round(duration_ns / 1e6, 2) AS ms, trace_id FROM read_parquet('s3://{{ s3_bucket }}/tel/traces/**/*.parquet') ORDER BY ms DESC LIMIT 20;"

# Delete the demo's telemetry output (local files and the gateway's directory)
clean:
    rm -rf {{ telemetry }} {{ s3_root }}
