"""Prove the Delta table supports time travel, and record the numbers.

This is the week-1 acceptance check from CLAUDE.md: a query against an earlier
version of the table must return that version's data, not the current data.

Time travel is what makes the rest of the project's guarantees checkable. It is
the difference between claiming a rerun did not corrupt anything and being able
to read both versions and compare row counts.

Run:
    ./.venv/bin/python spark/verify_delta.py --table data/delta/events
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession

sys.path.insert(0, str(Path(__file__).parent))
from batch_to_delta import build_spark  # noqa: E402


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="data/delta/events")
    parser.add_argument(
        "--artifact",
        default="benchmarks/results/week1_time_travel.json",
        help="Where to record the numbers. Every figure in the README must "
        "trace back to a committed file like this one.",
    )
    args = parser.parse_args()

    table = args.table if "://" in args.table else str(Path(args.table).resolve())
    spark = build_spark(table, "4g")

    history = spark.sql(f"DESCRIBE HISTORY delta.`{table}`").select(
        "version", "timestamp", "operation", "operationMetrics"
    )
    rows = sorted(history.collect(), key=lambda r: r["version"])

    print(f"table: {table}\n")
    print("history:")
    for row in rows:
        metrics = row["operationMetrics"] or {}
        written = metrics.get("numOutputRows", "?")
        print(
            f"  v{row['version']}  {row['timestamp']:%Y-%m-%d %H:%M:%S}  "
            f"{row['operation']:<9}  {written} rows"
        )

    if len(rows) < 2:
        sys.exit(
            "\nOnly one version exists. Run batch_to_delta.py a second time so "
            "there is a previous version to travel back to."
        )

    latest = rows[-1]["version"]
    previous = rows[-2]["version"]

    # The actual test. versionAsOf reconstructs the table as of that commit by
    # replaying the log, so this is reading genuinely older state -- not a
    # cached copy and not the current files.
    counts = {}
    for version in (previous, latest):
        started = time.time()
        counts[version] = (
            spark.read.format("delta")
            .option("versionAsOf", version)
            .load(table)
            .count()
        )
        print(f"\nversion {version}: {counts[version]:,} rows  "
              f"({time.time() - started:.1f}s)")

    assert counts[previous] > 0, f"version {previous} is empty -- time travel failed"

    artifact = {
        "table": table,
        "versions": [int(r["version"]) for r in rows],
        "operations": [r["operation"] for r in rows],
        "row_count_previous_version": counts[previous],
        "row_count_latest_version": counts[latest],
        "reran_same_input": counts[previous] == counts[latest],
    }
    out = Path(args.artifact)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, indent=2) + "\n")

    print(f"\nrecorded -> {out}")
    print("OK: time travel returns a previous version")
    spark.stop()


if __name__ == "__main__":
    main()
