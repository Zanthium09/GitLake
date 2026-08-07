"""Read GitHub Archive JSON, flatten it, write a Delta table by event_date.

This is the batch path. Week 2 replaces the source with Kafka, but the
flattening and the Delta write stay the same, which is why they live here
rather than inside a streaming job.

Run locally (fast, no network):
    ./.venv/bin/python spark/batch_to_delta.py --input 'data/raw/2024-01-15-*.json.gz'

Run against S3:
    ./.venv/bin/python spark/batch_to_delta.py \
        --input 'data/raw/2024-01-15-*.json.gz' --output s3a://BUCKET/delta/events

Rerunning the same day is safe: the write uses replaceWhere, so the day's
partition is replaced rather than appended to. Without it a second run would
silently double every row.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

sys.path.insert(0, str(Path(__file__).parent))
from schema import EVENT_SCHEMA, KNOWN_EVENT_TYPES  # noqa: E402

DELTA_PACKAGE = "io.delta:delta-spark_2.12:3.1.0"

# hadoop-aws must match the Hadoop version PySpark bundles (3.3.4 for Spark
# 3.5.0). A mismatch produces NoSuchMethodError deep in the S3A client, which
# reads like a network problem and is not one. The aws-java-sdk-bundle version
# is the one hadoop-aws 3.3.4 was built against.
HADOOP_AWS_PACKAGES = (
    "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262"
)


def build_spark(output_path: str, driver_memory: str) -> SparkSession:
    """Create a Spark session, adding S3 support only when writing to s3a://.

    The hadoop-aws and AWS SDK jars are ~200 MB. Pulling them for a local run
    wastes a download and slows every iteration, so they are only requested
    when the output path actually needs them.
    """
    needs_s3 = output_path.startswith("s3a://")
    packages = DELTA_PACKAGE + ("," + HADOOP_AWS_PACKAGES if needs_s3 else "")

    builder = (
        SparkSession.builder.appName("gitlake-batch-to-delta")
        .master("local[*]")
        .config("spark.jars.packages", packages)
        # Registers Delta's SQL parser and the catalog that makes
        # `format("delta")` and time travel work.
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.driver.memory", driver_memory)
        # Spark's default of 200 shuffle partitions is sized for a cluster. On
        # one machine it produces 200 tiny tasks whose scheduling costs more
        # than the work.
        .config("spark.sql.shuffle.partitions", "16")
    )

    if needs_s3:
        region = os.getenv("AWS_REGION", "").strip()
        builder = (
            builder.config(
                "spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem"
            )
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"].strip())
            .config(
                "spark.hadoop.fs.s3a.secret.key",
                os.environ["AWS_SECRET_ACCESS_KEY"].strip(),
            )
            # Endpoint must name the bucket's own region. Pointing at the wrong
            # one returns 301 Moved Permanently partway through a write.
            .config("spark.hadoop.fs.s3a.endpoint", f"s3.{region}.amazonaws.com")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def flatten(events: DataFrame) -> DataFrame:
    """Turn the nested event structure into one flat row per event.

    actor/repo/org are plain structs and just get their fields promoted.
    payload is the awkward one: its fields only exist for the event types that
    use them, so most of these columns are null on most rows. That is the
    intended shape -- see the module docstring in schema.py.

    event_date is derived here rather than at write time because it is the
    partition column, and a partition column computed inside the writer cannot
    be used to prune reads.
    """
    return events.select(
        F.col("id").alias("event_id"),
        F.col("type").alias("event_type"),
        F.col("created_at"),
        F.to_date("created_at").alias("event_date"),
        F.col("public").alias("is_public"),
        F.col("actor.id").alias("actor_id"),
        F.col("actor.login").alias("actor_login"),
        F.col("actor.display_login").alias("actor_display_login"),
        F.col("repo.id").alias("repo_id"),
        F.col("repo.name").alias("repo_name"),
        F.col("org.id").alias("org_id"),
        F.col("org.login").alias("org_login"),
        F.col("payload.action").alias("action"),
        F.col("payload.ref").alias("ref"),
        F.col("payload.ref_type").alias("ref_type"),
        F.col("payload.push_id").alias("push_id"),
        F.col("payload.size").alias("push_size"),
        F.col("payload.distinct_size").alias("push_distinct_size"),
        F.col("payload.head").alias("push_head"),
        F.col("payload.before").alias("push_before"),
        F.col("payload.number").alias("pr_number"),
        F.col("payload.pull_request.id").alias("pr_id"),
        F.col("payload.pull_request.state").alias("pr_state"),
        F.col("payload.pull_request.merged").alias("pr_merged"),
        F.col("payload.pull_request.created_at").alias("pr_created_at"),
        F.col("payload.pull_request.merged_at").alias("pr_merged_at"),
        F.col("payload.issue.id").alias("issue_id"),
        F.col("payload.issue.number").alias("issue_number"),
        F.col("payload.issue.state").alias("issue_state"),
        F.col("payload.comment.id").alias("comment_id"),
        F.col("payload.forkee.id").alias("forkee_id"),
        F.col("payload.forkee.full_name").alias("forkee_full_name"),
        F.col("payload.release.id").alias("release_id"),
        F.col("payload.release.tag_name").alias("release_tag"),
        F.col("payload.member.id").alias("member_id"),
        F.col("payload.member.login").alias("member_login"),
    )


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--input",
        required=True,
        help="Glob of .json.gz files, e.g. 'data/raw/2024-01-15-*.json.gz'.",
    )
    parser.add_argument(
        "--output",
        default="data/delta/events",
        help="Delta table path. Local dir or s3a://bucket/prefix.",
    )
    parser.add_argument("--driver-memory", default="8g")
    args = parser.parse_args()

    # Delta's path syntax (delta.`...`) cannot resolve a relative path, so a
    # local target is made absolute here. URLs are left alone.
    output = args.output if "://" in args.output else str(Path(args.output).resolve())

    spark = build_spark(output, args.driver_memory)
    started = time.time()

    raw = spark.read.schema(EVENT_SCHEMA).json(args.input)
    flat = flatten(raw).cache()

    rows = flat.count()
    dates = [r["event_date"] for r in flat.select("event_date").distinct().collect()]
    dates = sorted(d for d in dates if d is not None)

    if not dates:
        sys.exit("No rows read. Check the --input glob.")

    unknown = {r["event_type"] for r in flat.select("event_type").distinct().collect()}
    unknown -= KNOWN_EVENT_TYPES
    if unknown:
        sys.exit(f"Unknown event types, schema needs updating: {sorted(unknown)}")

    print(f"read      : {rows:,} events")
    print(f"dates     : {dates[0]} .. {dates[-1]} ({len(dates)} day(s))")
    print(f"writing   : {output}")

    # replaceWhere makes a rerun replace the day rather than append to it.
    # Overwriting the whole table would be simpler but would destroy other
    # days, and plain append would double-count on the second run.
    date_filter = " OR ".join(f"event_date = '{d}'" for d in dates)
    (
        flat.write.format("delta")
        .mode("overwrite")
        .partitionBy("event_date")
        .option("replaceWhere", date_filter)
        .save(output)
    )

    version = (
        spark.sql(f"DESCRIBE HISTORY delta.`{output}`")
        .selectExpr("max(version) AS v")
        .collect()[0]["v"]
    )
    written = spark.read.format("delta").load(output).count()

    print(f"\nwrote     : {written:,} rows at Delta version {version}")
    print(f"elapsed   : {time.time() - started:.1f}s")
    if written != rows:
        sys.exit(f"Row count mismatch: read {rows:,}, table holds {written:,}")

    spark.stop()


if __name__ == "__main__":
    main()
