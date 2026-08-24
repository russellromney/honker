"""SQLModel, as guides/orm/python.mdx shows it."""

import json
import os

import honker
from sqlalchemy import event
from sqlmodel import Session, create_engine, text

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
    with Session(engine) as session:
        result = session.exec(text(named), params=params).scalar_one()
        session.commit()
        return result


run(scalar, "sm")


def prove_atomicity():
    with Session(engine) as session:
        session.execute(text("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)"))
        session.commit()

    with Session(engine) as session:
        session.execute(
            text("INSERT INTO orders (id, user_id) VALUES (:id, :uid)"),
            {"id": 42, "uid": 1},
        )
        job_id = session.exec(
            text("SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL)"),
            params={"q": "sm_atomic", "p": json.dumps({"order_id": 42}), "prio": 0, "max": 3},
        ).scalar_one()
        session.commit()

    with Session(engine) as session:
        assert session.exec(text("SELECT COUNT(*) FROM orders WHERE id = 42")).scalar_one() == 1
        job = session.exec(text("SELECT honker_get_job(:id)"), params={"id": job_id}).scalar_one()
        assert job and "order_id" in job

    with Session(engine) as session:
        session.execute(
            text("INSERT INTO orders (id, user_id) VALUES (:id, :uid)"),
            {"id": 43, "uid": 1},
        )
        rolled = session.exec(
            text("SELECT honker_enqueue(:q, :p, NULL, NULL, :prio, :max, NULL)"),
            params={"q": "sm_atomic", "p": json.dumps({"order_id": 43}), "prio": 0, "max": 3},
        ).scalar_one()
        session.rollback()

    with Session(engine) as session:
        assert session.exec(text("SELECT COUNT(*) FROM orders WHERE id = 43")).scalar_one() == 0
        job = session.exec(text("SELECT honker_get_job(:id)"), params={"id": rolled}).scalar_one()
        assert not job


prove_atomicity()
print("PASS sqlmodel")
