"""SQLAlchemy / SQLModel, as guides/orm/python.mdx shows it.

Both share this wiring: the connect event loads the extension onto the
raw DB-API connection. SQLModel's Session subclasses SQLAlchemy's, so
the extension path is identical and only the wrapper on top differs.
"""

import json
import os
import sys

import honker
from sqlalchemy import create_engine, event, text

DB = os.environ["HONKER_TEST_DB"]
engine = create_engine(f"sqlite:///{DB}")


@event.listens_for(engine, "connect")
def _load_honker(conn, _):
    honker.load_extension(conn)
    conn.execute("SELECT honker_bootstrap()")


with engine.connect() as conn:
    # Bound parameters, not literals: SQLAlchemy's own binding layer is
    # the thing under test, the same way better-sqlite3's REAL binding
    # was for Node.
    payload = json.dumps({"to": "alice@example.com"})
    job_id = conn.execute(
        text(
            "SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL) AS id"
        ),
        {"q": "emails", "p": payload, "prio": 0, "max": 3},
    ).scalar_one()
    assert job_id > 0, f"expected a job id, got {job_id}"

    claimed = json.loads(
        conn.execute(
            text("SELECT honker_claim_batch(:q, :w, :n, :t) AS jobs"),
            {"q": "emails", "w": "w1", "n": 8, "t": 300},
        ).scalar_one()
    )
    assert len(claimed) == 1, f"expected one claimed job, got {claimed}"
    assert claimed[0]["id"] == job_id
    assert json.loads(claimed[0]["payload"])["to"] == "alice@example.com"

    acked = conn.execute(
        text("SELECT honker_ack(:id, :w) AS ok"), {"id": job_id, "w": "w1"}
    ).scalar_one()
    assert acked == 1, "ack must match the claim"

print("PASS sqlalchemy", file=sys.stderr)
print("PASS sqlalchemy")
