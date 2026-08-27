#!/usr/bin/env python3
"""Measure queue-event costs through the public Python queue API.

Build/install the local binding first, then run:

    .venv/bin/python bench/queue_events_bench.py --jobs 10000

This reports disabled and enabled throughput. The CI regression assertion for
the disabled batch path lives in ``tests/test_performance_floors.py``; this
script is for comparing fuller before/after profiles without flaky wall-clock
thresholds in every test job.
"""

from __future__ import annotations

import argparse
import json
import tempfile
import time
from pathlib import Path

import honker


def configure(db, *, enabled: bool, include_payload: bool) -> None:
    with db.transaction() as tx:
        tx.query(
            "SELECT honker_queue_events_configure(?, 1000000, ?)",
            [int(enabled), int(include_payload)],
        )


def run_mode(directory: Path, mode: str, jobs: int) -> dict[str, float | str]:
    db_path = str(directory / f"{mode}.db")
    db = honker.open(db_path)
    enabled = mode != "disabled"
    configure(db, enabled=enabled, include_payload=mode == "enabled-payload")
    # Python intentionally has no public queue-events binding yet. Reopen after
    # the raw transactional configure call so the timed writer starts with a
    # fresh core cache, matching a normal producer process.
    db.close()
    db = honker.open(db_path)
    queue = db.queue("bench", visibility_timeout_s=300)
    payload = {"id": 1, "kind": "queue-event-benchmark"}

    started = time.perf_counter()
    with db.transaction() as tx:
        for _ in range(jobs):
            queue.enqueue(payload, tx=tx)
    enqueue_seconds = time.perf_counter() - started

    claimed = queue.claim_batch("bench-worker", jobs)
    ids = [job.id for job in claimed]
    started = time.perf_counter()
    acked = queue.ack_batch(ids, "bench-worker")
    ack_seconds = time.perf_counter() - started
    assert acked == jobs
    db.close()

    return {
        "mode": mode,
        "enqueue_jobs_per_second": jobs / enqueue_seconds,
        "enqueue_seconds": enqueue_seconds,
        "ack_jobs_per_second": jobs / ack_seconds,
        "ack_seconds": ack_seconds,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--jobs", type=int, default=10_000)
    args = parser.parse_args()
    if args.jobs < 1:
        parser.error("--jobs must be positive")

    with tempfile.TemporaryDirectory(prefix="honker-queue-events-bench-") as temp:
        directory = Path(temp)
        results = [
            run_mode(directory, mode, args.jobs)
            for mode in ("disabled", "enabled", "enabled-payload")
        ]
    print(json.dumps({"jobs": args.jobs, "results": results}, indent=2))


if __name__ == "__main__":
    main()
