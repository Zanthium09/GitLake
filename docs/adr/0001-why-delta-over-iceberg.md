# ADR-0001: Delta Lake over Apache Iceberg

**Status:** Accepted
**Date:** 2026-08-03

## Context

The pipeline needs a table format over raw S3 object storage. Parquet files
alone are not enough: two concurrent writers can leave a half-written
directory, a failed Spark job leaves orphaned files that the next reader
silently counts, and there is no way to ask what the table looked like an
hour ago.

A table format solves this with a transaction log alongside the data. The
two realistic candidates are Delta Lake and Apache Iceberg. Both give ACID
commits, snapshot isolation, time travel, and schema evolution over object
storage. On capability alone the decision is close to a coin flip for a
project at this scale.

Two project-specific requirements narrow it:

1. Week 2 requires exactly-once semantics from a Spark Structured Streaming
   job writing to S3, surviving a mid-run kill.
2. Week 4 requires provably idempotent backfill — reprocessing the same
   window twice must produce identical row counts.

## Decision

Use Delta Lake 3.1.0 with Spark 3.5.0.

Three reasons, in order of weight:

**Streaming writer integration is first-class.** Delta's Spark sink commits
the streaming checkpoint offset and the data files in the same transaction.
Kill the job mid-batch and restart, and the partially written batch is not
in the log, so it is not visible and gets recomputed. That is the week-2
acceptance criterion, and Delta gives it without extra machinery. Iceberg's
Spark streaming sink is capable but historically less exercised, and the
failure modes are less documented.

**`MERGE INTO` is the idempotency primitive.** Week 4's backfill guarantee is
an upsert keyed on event id. Delta's `MERGE INTO` is mature, well documented
for the exact "reprocess a partition without duplicating" case, and works
identically from SQL and the Python API.

**Version alignment is not a research project.** Delta 3.1.0 pairs with Spark
3.5.x as a documented, tested combination — one `spark.jars.packages`
coordinate and two config lines. Iceberg needs a catalog decision (Hadoop,
Hive, Glue, REST) before the first write, and picking one is a second
architecture decision that this project does not need to spend a week on.

## Alternatives considered

**Apache Iceberg.** Genuinely better on two axes: hidden partitioning removes
a whole class of "query forgot the partition predicate" mistakes, and the
engine-neutral spec means Trino, Flink, and Snowflake read it natively.
Neither pays off here — this pipeline has exactly one writer and one engine
(Spark), so engine neutrality buys nothing, and partitioning by `event_date`
is simple enough that hidden partitioning solves a problem the project does
not have. Rejected as the right tool for a problem one size larger.

**Plain Parquet with manual partition directories.** Rejected. No atomic
commits means a failed write leaves partial data that the next read counts
as real, which makes the week-4 idempotency proof impossible to state
honestly.

**Hudi.** Rejected without deep evaluation. Strong at streaming upserts and
CDC, but CDC is explicitly out of scope, and it is named in job postings far
less often than Delta.

## Consequences

**Cost — lock-in to the Spark/Databricks ecosystem.** Delta outside Spark
means `delta-rs` or a connector, both less mature than the Spark path. If
this pipeline ever needed Flink or Trino, that would be a real migration.
Accepted: the stack is fixed and Spark-only by design.

**Cost — Snowflake reads Delta through an external table, not natively.**
Week 3 loads via `COPY INTO` from an external stage over the Parquet files
rather than reading the Delta log. In practice this means Snowflake sees a
point-in-time copy, not a live view, and time travel stays on the S3 side.
Acceptable because the warehouse leg is batch-loaded anyway.

**Cost — the transaction log accumulates.** Every commit writes a JSON entry
to `_delta_log/`. Over a week of streaming micro-batches this grows enough
that listing gets slow without periodic checkpointing. Delta checkpoints
every 10 commits automatically, so this needs no action at this scale, but
it would at production volume.

**Benefit — `DESCRIBE HISTORY` is free auditability.** Every write records
operation type, row counts, and timestamp. That turns the week-2 and week-4
acceptance criteria from claims into queries someone else can rerun.
