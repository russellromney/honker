"""Django, as guides/orm/python.mdx shows it."""

# docs:start example

import json
import os

import django
from django.conf import settings

settings.configure(
    DATABASES={
        "default": {
            "ENGINE": "django.db.backends.sqlite3",
            "NAME": os.environ["HONKER_TEST_DB"],
        }
    },
    INSTALLED_APPS=[],
)
django.setup()

import honker  # noqa: E402
from django.db import connection, transaction  # noqa: E402
from django.db.backends.signals import connection_created  # noqa: E402
from django.dispatch import receiver  # noqa: E402

from surface import run  # noqa: E402


@receiver(connection_created)
def _load_honker(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    raw = connection.connection
    honker.load_extension(raw)
    raw.execute("SELECT honker_bootstrap()")


def scalar(sql, args):
    with connection.cursor() as cur:
        cur.execute(sql.replace("?", "%s"), args)
        row = cur.fetchone()
        return None if row is None else row[0]


run(scalar, "dj")


def prove_atomicity():
    with connection.cursor() as cur:
        cur.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")

    with transaction.atomic():
        with connection.cursor() as cur:
            cur.execute("INSERT INTO orders (id, user_id) VALUES (%s, %s)", [42, 1])
            cur.execute(
                "SELECT honker_enqueue(%s, %s, NULL, NULL, %s, %s, NULL)",
                ["dj_atomic", json.dumps({"order_id": 42}), 0, 3],
            )
            job_id = cur.fetchone()[0]

    with connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders WHERE id = %s", [42])
        assert cur.fetchone()[0] == 1
        cur.execute("SELECT honker_get_job(%s)", [job_id])
        assert "order_id" in (cur.fetchone()[0] or "")

    try:
        with transaction.atomic():
            with connection.cursor() as cur:
                cur.execute("INSERT INTO orders (id, user_id) VALUES (%s, %s)", [43, 1])
                cur.execute(
                    "SELECT honker_enqueue(%s, %s, NULL, NULL, %s, %s, NULL)",
                    ["dj_atomic", json.dumps({"order_id": 43}), 0, 3],
                )
                rolled = cur.fetchone()[0]
            raise RuntimeError("rollback")
    except RuntimeError as exc:
        if str(exc) != "rollback":
            raise

    with connection.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM orders WHERE id = %s", [43])
        assert cur.fetchone()[0] == 0
        cur.execute("SELECT honker_get_job(%s)", [rolled])
        assert not cur.fetchone()[0]


prove_atomicity()
print("PASS django")
# docs:end example
