"""Check a Delta table for duplicate event_id rows.

Run after any streaming test, especially a kill-and-restart, to turn "I
think it worked" into a number.

    ./.venv/bin/python spark/verify_no_duplicates.py --table data/delta/events
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent))
from batch_to_delta import build_spark  # noqa: E402


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--table", default="data/delta/events")
    args = parser.parse_args()

    table = args.table if "://" in args.table else str(Path(args.table).resolve())
    spark = build_spark(table, "4g")

    df = spark.read.format("delta").load(table)
    total = df.count()
    distinct = df.select("event_id").distinct().count()
    duplicates = total - distinct

    print(f"table      : {table}")
    print(f"total rows : {total:,}")
    print(f"distinct   : {distinct:,}")
    print(f"duplicates : {duplicates:,}")

    if duplicates:
        print("\nworst offenders:")
        dupes = (
            df.groupBy("event_id")
            .count()
            .filter(F.col("count") > 1)
            .orderBy(F.desc("count"))
        )
        dupes.show(10, truncate=False)
        sys.exit(f"FAIL: {duplicates:,} duplicate event_id rows")

    print("\nOK: zero duplicates")
    spark.stop()


if __name__ == "__main__":
    main()
