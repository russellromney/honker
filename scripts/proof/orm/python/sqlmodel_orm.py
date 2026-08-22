"""SQLModel, as guides/orm/python.mdx shows it.

SQLModel's Session inherits from SQLAlchemy's, so this proves the
documented SQLModel entry point reaches the same working extension
rather than assuming the SQLAlchemy scenario covers it.
"""

import json
import os

import honker
from sqlalchemy import event
from sqlmodel import Session, create_engine, text

DB = os.environ["HONKER_TEST_DB"]
engine = create_engine(f"sqlite:///{DB}")


@event.listens_for(engine, "connect")
def _load_honker(conn, _):
    honker.load_extension(conn)
    conn.execute("SELECT honker_bootstrap()")


with Session(engine) as session:
    payload = json.dumps({"to": "alice@example.com"})
    job_id = session.exec(
        text("SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL) AS id"),
        params={"q": "emails", "p": payload, "prio": 0, "max": 3},
    ).scalar_one()
    assert job_id > 0, f"expected a job id, got {job_id}"

    claimed = json.loads(
        session.exec(
            text("SELECT honker_claim_batch(:q, :w, :n, :t) AS jobs"),
            params={"q": "emails", "w": "w1", "n": 8, "t": 300},
        ).scalar_one()
    )
    assert len(claimed) == 1
    assert claimed[0]["id"] == job_id

    acked = session.exec(
        text("SELECT honker_ack(:id, :w) AS ok"), params={"id": job_id, "w": "w1"}
    ).scalar_one()
    assert acked == 1

print("PASS sqlmodel")
