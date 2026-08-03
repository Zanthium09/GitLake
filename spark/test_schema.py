"""Check EVENT_SCHEMA against a real GitHub Archive hour.

Run:  ./.venv/bin/python spark/test_schema.py

Reading with an explicit schema fails quietly: rows that do not match are not
an error, they land in _corrupt_record and the count silently drops. So the
only honest check is to add that column and assert it stays empty.

Deliberately plain asserts, no pytest -- this is one check, and a test runner
is more moving parts than the thing it tests.
"""

from __future__ import annotations

import sys
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField

sys.path.insert(0, str(Path(__file__).parent))
from schema import EVENT_SCHEMA, KNOWN_EVENT_TYPES  # noqa: E402

SAMPLE = Path("data/raw/2024-01-15-0.json.gz")
CORRUPT_COLUMN = "_corrupt_record"


def main() -> None:
    if not SAMPLE.exists():
        sys.exit(f"{SAMPLE} missing. Run: python ingest/download.py --day 2024-01-15 --hours 0")

    spark = (
        SparkSession.builder.appName("test_schema")
        .master("local[4]")
        .config("spark.driver.memory", "4g")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    # PERMISSIVE keeps unparseable rows instead of dropping them, but only if
    # the schema has somewhere to put them.
    schema_with_corrupt = EVENT_SCHEMA.add(StructField(CORRUPT_COLUMN, StringType(), True))
    events = spark.read.schema(schema_with_corrupt).json(str(SAMPLE)).cache()

    total = events.count()
    corrupt = events.filter(F.col(CORRUPT_COLUMN).isNotNull()).count()
    print(f"rows            : {total:,}")
    print(f"corrupt         : {corrupt:,}")
    assert total == 197_313, f"expected 197,313 rows in this hour, got {total:,}"
    assert corrupt == 0, f"{corrupt:,} rows did not match EVENT_SCHEMA"

    # A null id/type/created_at would mean the envelope schema is wrong in a
    # way _corrupt_record does not catch.
    for column in ("id", "type", "created_at"):
        nulls = events.filter(F.col(column).isNull()).count()
        print(f"null {column:<11}: {nulls:,}")
        assert nulls == 0, f"{nulls:,} rows have a null {column}"

    seen = {row["type"] for row in events.select("type").distinct().collect()}
    unknown = seen - KNOWN_EVENT_TYPES
    print(f"event types     : {len(seen)}")
    assert not unknown, f"event types not covered by the schema: {sorted(unknown)}"

    # The union-schema claim: push fields populate for PushEvent and stay null
    # for everything else. If this fails the payload schema is mis-nested.
    push = events.filter(F.col("type") == "PushEvent")
    push_with_id = push.filter(F.col("payload.push_id").isNotNull()).count()
    print(f"PushEvent rows  : {push.count():,} ({push_with_id:,} with push_id)")
    assert push_with_id == push.count(), "some PushEvents have no payload.push_id"

    watch_with_push = (
        events.filter(F.col("type") == "WatchEvent")
        .filter(F.col("payload.push_id").isNotNull())
        .count()
    )
    print(f"WatchEvent leak : {watch_with_push:,}")
    assert watch_with_push == 0, "WatchEvent picked up push fields"

    # Nested extraction actually works, not just the top level.
    merged = events.filter(F.col("payload.pull_request.merged")).count()
    print(f"merged PRs      : {merged:,}")
    assert merged > 0, "no merged pull requests found -- nested struct likely wrong"

    print("\nOK: schema matches the data")
    spark.stop()


if __name__ == "__main__":
    main()
