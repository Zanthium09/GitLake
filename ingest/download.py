"""Fetch GitHub Archive hourly files for one UTC day.

GitHub Archive publishes one gzipped, newline-delimited JSON file per hour at
https://data.gharchive.org/YYYY-MM-DD-H.json.gz -- note H is NOT zero-padded,
so hour 0 is `-0.json.gz`, not `-00.json.gz`.

Downloading is kept separate from processing so that reprocessing does not
re-fetch ~1.4 GB per day over the network.
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

from dotenv import load_dotenv

BASE_URL = "https://data.gharchive.org"
CHUNK_BYTES = 1 << 20  # 1 MiB
MAX_ATTEMPTS = 3
BACKOFF_SECONDS = 5

# gharchive.org returns 403 for urllib's default `Python-urllib/x.y` agent.
# Identify the project honestly rather than spoofing a browser, so the host
# can rate-limit or contact the owner if this ever misbehaves.
USER_AGENT = "gitlake/0.1 (+https://github.com/Zanthium09/GitLake)"


def hour_url(day: str, hour: int) -> str:
    """Build the archive URL for one hour.

    Hour is deliberately not zero-padded -- gharchive returns 404 for
    `2024-01-15-00.json.gz` but serves `2024-01-15-0.json.gz`.
    """
    return f"{BASE_URL}/{day}-{hour}.json.gz"


def download_hour(day: str, hour: int, dest_dir: Path) -> tuple[Path, bool]:
    """Download one hourly file. Returns (path, was_downloaded).

    Idempotent: an existing non-empty file is left alone, so rerunning after
    a partial failure only fetches what is actually missing.

    Writes to a `.part` sibling and renames on success. os.replace within one
    filesystem is atomic, so an interrupted run can never leave a truncated
    file that the next run mistakes for a complete one.
    """
    dest = dest_dir / f"{day}-{hour}.json.gz"
    if dest.exists() and dest.stat().st_size > 0:
        return dest, False

    url = hour_url(day, hour)
    tmp = dest.with_suffix(".gz.part")

    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})

    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            with urllib.request.urlopen(request, timeout=60) as response:
                with tmp.open("wb") as handle:
                    shutil.copyfileobj(response, handle, CHUNK_BYTES)
            os.replace(tmp, dest)
            return dest, True
        except (urllib.error.URLError, TimeoutError, OSError) as exc:
            tmp.unlink(missing_ok=True)
            # Retry only what a retry can fix. A 4xx means the request itself
            # is wrong, so resending it unchanged just wastes time -- except
            # 429, which is explicitly "wait and try again".
            if isinstance(exc, urllib.error.HTTPError) and 400 <= exc.code < 500:
                if exc.code != 429:
                    raise SystemExit(f"{url} -> HTTP {exc.code} {exc.reason}")
            if attempt == MAX_ATTEMPTS:
                raise SystemExit(f"{url} failed after {MAX_ATTEMPTS} attempts: {exc}")
            time.sleep(BACKOFF_SECONDS * attempt)

    raise AssertionError("unreachable")


def download_day(day: str, dest_dir: Path, hours: list[int]) -> None:
    dest_dir.mkdir(parents=True, exist_ok=True)
    fetched = skipped = 0

    for hour in hours:
        path, was_downloaded = download_hour(day, hour, dest_dir)
        size_mb = path.stat().st_size / 1_048_576
        if was_downloaded:
            fetched += 1
            print(f"  fetched  {path.name}  {size_mb:6.1f} MB")
        else:
            skipped += 1
            print(f"  skipped  {path.name}  {size_mb:6.1f} MB  (already present)")

    total_mb = sum(p.stat().st_size for p in dest_dir.glob(f"{day}-*.json.gz")) / 1_048_576
    print(f"\n{day}: {fetched} fetched, {skipped} skipped, {total_mb:.1f} MB on disk")


def main() -> None:
    load_dotenv()

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--day",
        default=os.getenv("GHARCHIVE_DATE"),
        help="UTC day as YYYY-MM-DD. Defaults to GHARCHIVE_DATE in .env.",
    )
    parser.add_argument(
        "--hours",
        default="0-23",
        help="Hour range like '0-23' or a single hour like '3'. Default: 0-23.",
    )
    parser.add_argument(
        "--dest",
        type=Path,
        default=Path("data/raw"),
        help="Directory to write into. Default: data/raw (gitignored).",
    )
    args = parser.parse_args()

    if not args.day:
        sys.exit("No day given. Pass --day YYYY-MM-DD or set GHARCHIVE_DATE in .env.")

    if "-" in args.hours:
        start, end = (int(part) for part in args.hours.split("-", 1))
        hours = list(range(start, end + 1))
    else:
        hours = [int(args.hours)]

    if not all(0 <= hour <= 23 for hour in hours):
        sys.exit(f"Hours must be within 0-23, got {args.hours}.")

    print(f"Downloading {args.day} hours {hours[0]}-{hours[-1]} into {args.dest}/")
    download_day(args.day, args.dest, hours)


if __name__ == "__main__":
    main()
