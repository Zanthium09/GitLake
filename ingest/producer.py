"""Replay archived GitHub events into Kafka as if they were arriving live.

The archive is static files, but the pipeline is meant to consume a stream. So
this reads the files and publishes each event as its own Kafka message, rate
limited, which gives Spark Structured Streaming something that behaves like a
real feed -- messages arriving over time, in batches of varying size, with a
consumer that can fall behind.

    ./.venv/bin/python ingest/producer.py --input 'data/raw/2024-01-15-0.json.gz'
    ./.venv/bin/python ingest/producer.py --input '...' --rate 20000 --limit 500000

Messages are sent with no key, so Kafka round-robins them across partitions.
Keying by repo id would preserve per-repo ordering, but nothing downstream
depends on that -- the analyses aggregate by day and hour -- and it would cost
a JSON parse of every line here plus partition skew from busy repos. Even
distribution across partitions is worth more, because it is what lets Spark
read the topic in parallel.

Ctrl+C is a supported way to stop: the run flushes what it has and reports the
count. Week 2's acceptance test depends on interrupting this cleanly.
"""

from __future__ import annotations

import argparse
import glob
import gzip
import os
import signal
import sys
import time
from pathlib import Path

from dotenv import load_dotenv
from kafka import KafkaProducer

REPORT_EVERY = 50_000
_stopping = False


def _handle_interrupt(signum, frame) -> None:
    """Flip a flag rather than raising, so the send loop can flush and report."""
    global _stopping
    _stopping = True
    print("\ninterrupted -- flushing", flush=True)


def build_producer(bootstrap: str) -> KafkaProducer:
    """Create a producer tuned for throughput over latency.

    linger_ms batches messages before sending. The default of 0 sends each
    message on its own, which for millions of small events means millions of
    tiny network round trips. Waiting 50ms to fill a batch is invisible at this
    scale and roughly an order of magnitude faster.

    acks=1 waits for the leader to write but not for replicas. There is one
    broker here so acks=all would mean the same thing at higher cost; on a real
    cluster this is the durability knob worth arguing about.
    """
    return KafkaProducer(
        bootstrap_servers=bootstrap,
        linger_ms=50,
        batch_size=256 * 1024,
        acks=1,
        compression_type="gzip",
        max_in_flight_requests_per_connection=5,
    )


def iter_events(paths: list[Path]):
    """Yield raw JSON lines as bytes, without parsing them.

    Deliberately no json.loads here. The line is already valid JSON and Spark
    parses it on the other side with an explicit schema, so parsing here would
    cost minutes of CPU across millions of events and buy nothing.
    """
    for path in paths:
        with gzip.open(path, "rb") as handle:
            for line in handle:
                line = line.strip()
                if line:
                    yield line


def main() -> None:
    load_dotenv(Path(__file__).resolve().parent.parent / ".env")

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, help="Glob of .json.gz files.")
    parser.add_argument(
        "--topic", default=os.getenv("KAFKA_TOPIC", "gitlake.events")
    )
    parser.add_argument(
        "--bootstrap",
        default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"),
    )
    parser.add_argument(
        "--rate",
        type=int,
        default=20_000,
        help="Target events per second. 0 means as fast as possible.",
    )
    parser.add_argument(
        "--limit", type=int, default=0, help="Stop after N events. 0 means all."
    )
    args = parser.parse_args()

    paths = sorted(Path(p) for p in glob.glob(args.input))
    if not paths:
        sys.exit(f"No files matched {args.input!r}.")

    signal.signal(signal.SIGINT, _handle_interrupt)
    producer = build_producer(args.bootstrap)

    print(f"topic     : {args.topic} @ {args.bootstrap}")
    print(f"files     : {len(paths)}")
    print(f"rate      : {'unlimited' if args.rate == 0 else f'{args.rate:,}/s'}\n")

    sent = 0
    started = time.time()

    for payload in iter_events(paths):
        if _stopping or (args.limit and sent >= args.limit):
            break

        producer.send(args.topic, value=payload)
        sent += 1

        # Rate limiting on the batch boundary, not per message. time.sleep()
        # cannot resolve the ~50 microseconds a per-message pause would need,
        # and calling it millions of times costs more than the wait itself.
        if args.rate and sent % 1000 == 0:
            expected = sent / args.rate
            drift = expected - (time.time() - started)
            if drift > 0:
                time.sleep(drift)

        if sent % REPORT_EVERY == 0:
            elapsed = time.time() - started
            print(f"  sent {sent:>9,}   {sent / elapsed:>9,.0f}/s", flush=True)

    producer.flush()
    producer.close()

    elapsed = time.time() - started
    print(f"\nsent      : {sent:,} events in {elapsed:.1f}s ({sent / elapsed:,.0f}/s)")
    if _stopping:
        print("stopped early by interrupt")


if __name__ == "__main__":
    main()
