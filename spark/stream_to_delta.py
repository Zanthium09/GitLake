"""Consume the Kafka event topic into Delta, exactly once.

Same flattening as batch_to_delta.py -- the parsing and column shape do not
change between batch and streaming, only where the rows come from. What is
new here is everything that makes a stream safe to kill and restart:

  trigger(availableNow=True)   process what is currently in Kafka, then exit.
                                Real streaming semantics (micro-batches, a
                                checkpoint, exactly-once) without a 24/7
                                process for Airflow to babysit -- it runs like
                                any other scheduled task.

  checkpointLocation on S3     records which Kafka offsets have been
                                committed, in the same transaction as the
                                Delta write. A kill between "wrote the data"
                                and "recorded the offset" cannot happen --
                                Structured Streaming only advances the offset
                                after the sink confirms the write. That is the
                                actual mechanism behind "kill and restart
                                produces zero duplicates", not a side effect
                                of using Kafka.

  withWatermark + dropDuplicates(event_id)
                                bounds how long the engine keeps state for
                                deduplication and uses that state to drop
                                true duplicate messages. See LateEventListener
                                for how the drop rate gets measured, and the
                                WATERMARK_DURATION comment below for why it is
                                26 hours, not the 10 minutes a genuinely
                                real-time feed would use.

Run:
    ./.venv/bin/python spark/stream_to_delta.py \
        --output data/delta/events --checkpoint data/checkpoints/events

    ./.venv/bin/python spark/stream_to_delta.py \
        --output s3a://BUCKET/delta/events \
        --checkpoint s3a://BUCKET/checkpoints/events
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import functions as F
from pyspark.sql.streaming import StreamingQueryListener

sys.path.insert(0, str(Path(__file__).parent))
from batch_to_delta import build_spark, flatten  # noqa: E402
from schema import EVENT_SCHEMA  # noqa: E402

# 26 hours, not 10 minutes. This pipeline replays a full HISTORICAL day
# through Kafka in ~15-20 minutes of wall-clock time, across 4 partitions
# with no ordering guarantee between them. A 10-minute watermark is the
# right number for a genuinely real-time feed, where "how late can one event
# arrive relative to another" is actually bounded by real-world jitter. It
# is the wrong number here: measured empirically at full scale (24 hours,
# 5,528,301 events), a 10-minute watermark dropped 30.4% of the day as
# "late" -- not straggling individual events, but ordinary partition
# consumption skew. If partition 0 happens to be a few micro-batches ahead
# of partition 3 at any point, the watermark (driven by the max event time
# seen so far, across all partitions) races ahead of partition 3's true
# position, and partition 3's still-legitimate, still-in-order events get
# judged "late" the moment they arrive. Widening the watermark past the
# entire replay window removes that false-positive path while still
# bounding dedup state and still catching genuinely stale data (a multi-day
# clock skew, a stale reprocess) -- the mechanism this pipeline needs to
# demonstrate stays intact, just calibrated to what this pipeline actually
# does: batch-replay a day, not stream events as they happen.
WATERMARK_DURATION = "26 hours"


class LateEventListener(StreamingQueryListener):
    """Sum numRowsDroppedByWatermark across every micro-batch.

    Structured Streaming drops a row from a stateful operator (dropDuplicates
    here) when its event time trails the watermark by more than
    WATERMARK_DURATION -- it is considered too late to safely deduplicate and
    is excluded before reaching the operator. That count is the late-event
    rate: not parse failures or bad data, but real events that arrived out of
    order by more than the tolerance this job allows.
    """

    def __init__(self) -> None:
        self.total_input_rows = 0
        self.total_dropped = 0
        self.batches = 0

    def onQueryStarted(self, event) -> None:  # noqa: N802
        pass

    def onQueryIdle(self, event) -> None:  # noqa: N802
        pass

    def onQueryProgress(self, event) -> None:  # noqa: N802
        progress = event.progress
        if progress.numInputRows == 0:
            return
        self.batches += 1
        self.total_input_rows += progress.numInputRows
        for op in progress.stateOperators:
            self.total_dropped += op.numRowsDroppedByWatermark

    def onQueryTerminated(self, event) -> None:  # noqa: N802
        pass


def build_stream(spark, topic: str, bootstrap: str, max_offsets_per_trigger: int):
    """Read Kafka, parse with the same schema batch_to_delta.py uses, flatten,
    then bound state with a watermark before deduplicating on event_id.
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        # Caps rows per micro-batch. Without this a fresh consumer with a
        # large backlog pulls everything into one batch, which both defeats
        # the point of "micro" and makes a kill-mid-run test impossible to
        # aim -- there would only be one batch to kill between.
        .option("maxOffsetsPerTrigger", max_offsets_per_trigger)
        .load()
    )

    parsed = raw.select(
        F.from_json(F.col("value").cast("string"), EVENT_SCHEMA).alias("e")
    ).select("e.*")

    flat = flatten(parsed)
    return flat.withWatermark("created_at", WATERMARK_DURATION).dropDuplicates(
        ["event_id"]
    )


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default="data/delta/events")
    parser.add_argument("--checkpoint", default="data/checkpoints/events")
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "gitlake.events")
    )
    parser.add_argument(
        "--bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    )
    parser.add_argument("--max-offsets-per-trigger", type=int, default=50_000)
    parser.add_argument("--driver-memory", default="8g")
    parser.add_argument(
        "--metrics-out",
        default="benchmarks/results/week2_watermark.json",
        help="Where to record the late-event rate. Empty string skips writing.",
    )
    args = parser.parse_args()

    output = args.output if "://" in args.output else str(Path(args.output).resolve())
    checkpoint = (
        args.checkpoint if "://" in args.checkpoint else str(Path(args.checkpoint).resolve())
    )

    # A local checkpoint is legal for testing but not the real design -- see
    # the module docstring on why the checkpoint's atomicity with the sink
    # write is what makes the exactly-once claim true, not a Kafka feature.
    if not checkpoint.startswith("s3a://"):
        print("WARNING: checkpoint is not on S3. Fine for local testing; the "
              "week-2 guarantee assumes a checkpoint that survives past this "
              "machine.", file=sys.stderr)

    spark = build_spark(
        output,
        args.driver_memory,
        extra_packages="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    )

    listener = LateEventListener()
    spark.streams.addListener(listener)

    stream = build_stream(spark, args.topic, args.bootstrap, args.max_offsets_per_trigger)

    started = time.time()
    query = (
        stream.writeStream.format("delta")
        .outputMode("append")
        .option("checkpointLocation", checkpoint)
        .partitionBy("event_date")
        .trigger(availableNow=True)
        .start(output)
    )
    query.awaitTermination()
    elapsed = time.time() - started

    total = spark.read.format("delta").load(output).count()
    late_rate = (
        listener.total_dropped / listener.total_input_rows
        if listener.total_input_rows
        else 0.0
    )

    print(f"\nbatches         : {listener.batches}")
    print(f"input rows      : {listener.total_input_rows:,}")
    print(f"dropped (late)  : {listener.total_dropped:,}  ({late_rate:.4%})")
    print(f"table rows      : {total:,}")
    print(f"elapsed         : {elapsed:.1f}s")

    if args.metrics_out:
        out = Path(args.metrics_out)
        out.parent.mkdir(parents=True, exist_ok=True)
        out.write_text(
            json.dumps(
                {
                    "batches": listener.batches,
                    "input_rows": listener.total_input_rows,
                    "dropped_by_watermark": listener.total_dropped,
                    "late_event_rate": late_rate,
                    "watermark_duration": WATERMARK_DURATION,
                    "table_rows_after_run": total,
                    "elapsed_seconds": elapsed,
                },
                indent=2,
            )
            + "\n"
        )
        print(f"recorded        : {out}")

    spark.stop()


if __name__ == "__main__":
    main()
