"""Type-checker gate for the Python binding's public type hints.

Nothing here runs. mypy checks it, and that is the whole point: #143 ships
`Queue[T]`, `Job[T]` and `JobSnapshot` as hints, and hints that nobody
checks rot silently. The Node binding gates its `wrapper.d.ts` with `tsc`;
this is the Python equivalent.

Run it against the *installed* honker, not the source tree:

    mypy --warn-unused-ignores scripts/proof/typing/check_payload_hints.py

Resolving through site-packages is load-bearing. A package without the PEP
561 `py.typed` marker is skipped entirely by mypy and pyright, and every
annotation below would resolve to `Any` — so this file also proves the
marker shipped. `assert_type` fails loudly against `Any`, which is exactly
the failure we want to see.

The `# type: ignore[...]` lines are the negative half of the proof. With
`--warn-unused-ignores`, an ignore that stops being needed is itself an
error, so each one asserts that mypy still rejects that line.
"""

from typing import Any, TypedDict, assert_type

import honker


class EmailPayload(TypedDict):
    recipient: str
    template: str


def check_queue_and_job(db: honker.Database) -> None:
    # `db.queue()` cannot infer a payload type from a queue name; the
    # annotation is what binds T.
    assert_type(db.queue("emails"), "honker.Queue[Any]")
    emails: honker.Queue[EmailPayload] = db.queue("emails")

    # Producer side: the queue's payload type constrains enqueue.
    emails.enqueue({"recipient": "a@example.com", "template": "welcome"})
    emails.enqueue({"wrong": "shape"})  # type: ignore[typeddict-item]
    emails.enqueue("not a payload at all")  # type: ignore[arg-type]

    # Consumer side.
    job = emails.claim_one("worker-1")
    assert_type(job, "honker.Job[EmailPayload] | None")
    if job is None:
        return
    assert_type(job.payload, EmailPayload)
    assert_type(job.payload["recipient"], str)
    job.payload["nope"]  # type: ignore[typeddict-item]

    batch = emails.claim_batch("worker-1", 10)
    assert_type(batch, "list[honker.Job[EmailPayload]]")


def check_job_fields(job: "honker.Job[EmailPayload]") -> None:
    """The twelve fields of #136, each with its real type.

    These used to resolve to `Any` — `Job.__init__` reads them out of an
    untyped row dict — which made the field list unusable to a checker.
    """
    assert_type(job.id, int)
    assert_type(job.queue_name, str)
    assert_type(job.payload, EmailPayload)
    assert_type(job.state, honker.JobState)
    assert_type(job.priority, int)
    assert_type(job.run_at, int)
    assert_type(job.worker_id, str)
    assert_type(job.claim_expires_at, "int | None")
    assert_type(job.attempts, int)
    assert_type(job.max_attempts, int)
    assert_type(job.created_at, int)
    assert_type(job.expires_at, "int | None")

    # `job.queue` is the Queue object the claim methods call, not the
    # `queue` column. The column is `job.queue_name`.
    assert_type(job.queue, "honker.Queue[EmailPayload]")
    assert_type(job.ack(), bool)


def check_snapshot(emails: "honker.Queue[EmailPayload]") -> None:
    row = emails.get_job(1)
    assert_type(row, "honker.JobSnapshot | None")
    if row is None:
        return

    assert_type(row["id"], int)
    assert_type(row["queue"], str)
    assert_type(row["state"], honker.JobState)
    assert_type(row["priority"], int)
    assert_type(row["run_at"], int)
    assert_type(row["attempts"], int)
    assert_type(row["max_attempts"], int)
    assert_type(row["created_at"], int)

    # Genuinely optional — None, never 0 and never "".
    assert_type(row["worker_id"], "str | None")
    assert_type(row["claim_expires_at"], "int | None")
    assert_type(row["expires_at"], "int | None")

    # A snapshot's payload is raw JSON *text*, unlike a claimed
    # `Job.payload`. #146 tracks reconciling that across bindings; if it
    # is ever decoded here, this line must change deliberately.
    assert_type(row["payload"], str)

    row["nope"]  # type: ignore[typeddict-item]
