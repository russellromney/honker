"""SQLAlchemy, as guides/orm/python.mdx shows it."""

import json
import os

import honker
from sqlalchemy import create_engine, event, text

from surface import qmark_to_named, run

DB = os.environ["HONKER_TEST_DB"]
engine = create_engine(f"sqlite:///{DB}")


@event.listens_for(engine, "connect")
def _load_honker(conn, _):
    honker.load_extension(conn)
    conn.execute("SELECT honker_bootstrap()")


def scalar(sql, args):
    named, names = qmark_to_named(sql)
    params = dict(zip(names, args))
    with engine.connect() as conn:
        result = conn.execute(text(named), params).scalar()
        conn.commit()
        return result


run(scalar, "sa")


def prove_atomicity():
    with engine.begin() as conn:
        conn.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)"))

    with engine.begin() as conn:
        conn.execute(text("INSERT INTO orders (id, user_id) VALUES (:id, :uid)"), {"id": 42, "uid": 1})
        job_id = conn.execute(
            text("SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL)"),
            {"q": "sa_atomic", "p": json.dumps({"order_id": 42}), "prio": 0, "max": 3},
        ).scalar_one()

    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM orders WHERE id = 42")).scalar_one() == 1
        job = conn.execute(text("SELECT honker_get_job(:id)"), {"id": job_id}).scalar()
        assert job and "order_id" in job

    with engine.connect() as conn:
        trans = conn.begin()
        conn.execute(text("INSERT INTO orders (id, user_id) VALUES (:id, :uid)"), {"id": 43, "uid": 1})
        rolled = conn.execute(
            text("SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL)"),
            {"q": "sa_atomic", "p": json.dumps({"order_id": 43}), "prio": 0, "max": 3},
        ).scalar_one()
        trans.rollback()

    with engine.connect() as conn:
        assert conn.execute(text("SELECT COUNT(*) FROM orders WHERE id = 43")).scalar_one() == 0
        job = conn.execute(text("SELECT honker_get_job(:id)"), {"id": rolled}).scalar()
        assert not job


prove_atomicity()
print("PASS sqlalchemy")
