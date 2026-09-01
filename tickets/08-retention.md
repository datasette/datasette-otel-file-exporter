# 08 — Stretch: retention pruning

Status: todo (stretch — skip for 0.1)

Files accumulate forever by default. That is arguably a feature (they're cheap, and
they're the archive), so retention ships **off by default** and this whole ticket can
lose to "document a find/aws-cli one-liner instead".

## If built

```yaml
plugins:
  datasette-otel-parquet:
    path: ./telemetry
    retention_days: 14   # absent → keep everything
```

- Prune on a timer on the exporter thread (piggyback the flush tick, check at most
  hourly): obstore `list` under `traces/`, delete files whose hour-partition path is
  older than the cutoff. Parse the partition from the *path* — never open files.
- Hour granularity is plenty. Delete errors: log once, try again next tick.
- Same code path must work for local and `url:` stores (obstore gives list/delete on
  both — that's why it earns a place here at all).

## The cheaper alternative (write this in the README either way)

```bash
find telemetry/traces -name '*.parquet' -mtime +14 -delete
# or lifecycle rules on the bucket, which S3/GCS/R2 all do natively and better
```

Bucket lifecycle rules make `retention_days` for `url:` stores almost pointless —
which is the strongest argument for skipping this ticket entirely and shipping the
paragraph above instead.

## Acceptance (if built)

- Files older than cutoff removed on the next tick, newer untouched; fake-clock test.
- No retention config → no list calls at all.
