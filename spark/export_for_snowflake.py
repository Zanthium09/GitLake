"""Write a clean, single-version snapshot of the Delta table for Snowflake to
read via COPY INTO.

Delta keeps every historical data file on S3 for time travel -- an overwrite
does not delete the files it replaces, it just stops referencing them in
_delta_log. That is correct and wanted for the pipeline's own use of the
table, but Snowflake's external stage has no idea _delta_log exists: COPY
INTO just lists every *.parquet file under the stage's URL and loads all of
them. Point it at delta/events/ directly and it silently loads every
superseded file from every run this table has ever seen, not the current
5.5M rows -- discovered when a COPY INTO there returned 11,451,228 rows
against a table that actually holds 5,528,301.

The fix is not to VACUUM the source table -- that permanently deletes the
older versions and would erase the week-1 time-travel proof for good. Instead
this reads the table's CURRENT snapshot (Delta already resolves that
correctly) and writes it, once, cleanly, to a separate path that only ever
holds one version. Run this before every Snowflake load; it is cheap relative
to the load itself and is the only way to keep file count matching row count
without touching the source table's history.

This export path is a different case from the source table, and gets
VACUUMed here -- found the hard way, when Airflow's daily DAG called this
script on every run (not once, the way it was tested manually in week 3) and
COPY INTO started returning 18,752,259 rows against a table holding 5,528,300
distinct events, a 3.4x inflation exactly matching three accumulated
export runs. Every overwrite of DEST leaves its own predecessor's files
behind for the identical reason SOURCE does, and COPY INTO has no more
_delta_log awareness of DEST than it does of SOURCE. But DEST has no reason
to ever need time travel -- it exists solely as a Snowflake staging copy,
rebuilt fresh before every load -- so RETAIN 0 HOURS here costs nothing and
closes the loop this script's own repeated use kept reopening.

Run:
    ./.venv/bin/python spark/export_for_snowflake.py
"""

from __future__ import annotations

import sys
import time
from pathlib import Path

from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent))
from batch_to_delta import build_spark  # noqa: E402

SOURCE = "s3a://gitlake-rushabh-mumbai/delta/events"
DEST = "s3a://gitlake-rushabh-mumbai/delta/events_snowflake"


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")
    spark = build_spark(DEST, "8g")

    started = time.time()
    # read().load() on a Delta path returns the current snapshot only --
    # Delta itself already resolves which files are live, which is exactly
    # the resolution COPY INTO cannot do against the raw path.
    current = spark.read.format("delta").load(SOURCE)
    row_count = current.count()

    (
        current.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .save(DEST)
    )

    written = spark.read.format("delta").load(DEST).count()

    # Safe at RETAIN 0 HOURS specifically because DEST is disposable -- see
    # the module docstring. The safety check is disabled because the normal
    # 7-day minimum exists to protect concurrent readers and time travel,
    # neither of which this path ever needs to support.
    spark.conf.set("spark.databricks.delta.retentionDurationCheck.enabled", "false")
    spark.sql(f"VACUUM delta.`{DEST}` RETAIN 0 HOURS")

    elapsed = time.time() - started

    print(f"source rows : {row_count:,}")
    print(f"export rows : {written:,}")
    print(f"elapsed     : {elapsed:.1f}s")
    if written != row_count:
        sys.exit(f"Row count mismatch: source {row_count:,}, export {written:,}")

    print(f"\nOK -> {DEST}")
    spark.stop()


if __name__ == "__main__":
    main()
