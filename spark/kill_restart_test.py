"""Prove the week-2 claim: kill the streaming job mid-run, restart it, zero
duplicate rows.

PySpark's Python process is a thin client -- the actual micro-batch execution
happens in a child JVM started via py4j. SIGKILLing only the Python process
can leave that JVM orphaned and still writing, which would make this test
report success without having tested anything. So the job is launched in its
own process group (preexec_fn=os.setsid) and killed with os.killpg, which
takes the JVM down with it -- a genuine hard crash, not a clean shutdown.

Run:
    ./.venv/bin/python spark/kill_restart_test.py
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
PYTHON = ROOT / ".venv" / "bin" / "python"
SCRIPT = ROOT / "spark" / "stream_to_delta.py"

KILL_AFTER_SECONDS = 20
MAX_OFFSETS_PER_TRIGGER = 5_000


def run_job(output: str, checkpoint: str) -> subprocess.Popen:
    cmd = [
        str(PYTHON),
        str(SCRIPT),
        "--output",
        output,
        "--checkpoint",
        checkpoint,
        "--max-offsets-per-trigger",
        str(MAX_OFFSETS_PER_TRIGGER),
        "--metrics-out",
        "",
    ]
    return subprocess.Popen(
        cmd,
        cwd=ROOT,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        preexec_fn=os.setsid,
    )


def count_committed_batches(checkpoint: str) -> int:
    if checkpoint.startswith("s3a://"):
        # Local commit counting only; the S3 run relies on the printed batch
        # count and the final duplicate check instead.
        return -1
    commits = Path(checkpoint) / "commits"
    if not commits.exists():
        return 0
    return len([p for p in commits.iterdir() if not p.name.endswith(".crc")])


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", default=str(ROOT / "data" / "delta" / "kill_restart_test"))
    parser.add_argument(
        "--checkpoint", default=str(ROOT / "data" / "checkpoints" / "kill_restart_test")
    )
    parser.add_argument(
        "--artifact",
        default=str(ROOT / "benchmarks" / "results" / "week2_kill_restart.json"),
    )
    args = parser.parse_args()
    output, checkpoint = args.output, args.checkpoint

    if not output.startswith("s3a://"):
        shutil.rmtree(output, ignore_errors=True)
    if not checkpoint.startswith("s3a://"):
        shutil.rmtree(checkpoint, ignore_errors=True)

    print(f"output          : {output}")
    print(f"checkpoint      : {checkpoint}")
    print(f"=== first run: will SIGKILL the whole process group after "
          f"{KILL_AFTER_SECONDS}s ===\n")

    proc = run_job(output, checkpoint)
    time.sleep(KILL_AFTER_SECONDS)

    still_running = proc.poll() is None
    if still_running:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
        proc.wait(timeout=15)
    output_before_kill, _ = proc.communicate() if not still_running else ("", "")
    if still_running:
        try:
            output_before_kill = proc.stdout.read()
        except Exception:
            output_before_kill = ""

    committed_before_restart = count_committed_batches(checkpoint)
    print(f"process was {'killed mid-run' if still_running else 'already finished'}")
    print(f"micro-batches committed before kill: {committed_before_restart}")

    if not still_running:
        sys.exit(
            "The job finished before the kill landed -- lower "
            "MAX_OFFSETS_PER_TRIGGER or KILL_AFTER_SECONDS so there is "
            "actually a run in progress to interrupt."
        )

    print("\n=== second run: same checkpoint, should resume and finish ===\n")
    proc2 = run_job(output, checkpoint)
    stdout2, _ = proc2.communicate()
    print(stdout2)

    if proc2.returncode != 0:
        sys.exit(f"restart failed with exit code {proc2.returncode}")

    print("=== verifying ===\n")
    verify = subprocess.run(
        [str(PYTHON), str(ROOT / "spark" / "verify_no_duplicates.py"),
         "--table", output],
        cwd=ROOT,
        capture_output=True,
        text=True,
    )
    print(verify.stdout)
    passed = verify.returncode == 0

    artifact = {
        "output": output,
        "checkpoint": checkpoint,
        "kill_after_seconds": KILL_AFTER_SECONDS,
        "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
        "process_was_killed_mid_run": still_running,
        "micro_batches_committed_before_kill": committed_before_restart,
        "restart_exit_code": proc2.returncode,
        "zero_duplicates": passed,
    }
    out = Path(args.artifact)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(artifact, indent=2) + "\n")
    print(f"recorded -> {out}")

    if not passed:
        sys.exit("FAIL: duplicates found after kill-and-restart")
    print("\nOK: killed mid-run, restarted, zero duplicate rows")


if __name__ == "__main__":
    main()
