"""Django, as guides/orm/python.mdx shows it.

Drives the real connection_created signal rather than loading the
extension by hand, so the documented receiver is what gets exercised.
"""

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

import honker  # noqa: E402  (must follow django.setup())
from django.db import connection  # noqa: E402
from django.db.backends.signals import connection_created  # noqa: E402
from django.dispatch import receiver  # noqa: E402


@receiver(connection_created)
def _load_honker(sender, connection, **kwargs):
    if connection.vendor != "sqlite":
        return
    raw = connection.connection  # underlying sqlite3.Connection
    honker.load_extension(raw)
    raw.execute("SELECT honker_bootstrap()")


with connection.cursor() as cur:
    payload = json.dumps({"to": "alice@example.com"})
    # Django's placeholder style, with every value bound.
    cur.execute(
        "SELECT honker_enqueue(%s, %s, NULL, NULL, %s, %s, NULL)",
        ["emails", payload, 0, 3],
    )
    job_id = cur.fetchone()[0]
    assert job_id > 0, f"expected a job id, got {job_id}"

    cur.execute("SELECT honker_claim_batch(%s, %s, %s, %s)", ["emails", "w1", 8, 300])
    claimed = json.loads(cur.fetchone()[0])
    assert len(claimed) == 1, f"expected one claimed job, got {claimed}"
    assert claimed[0]["id"] == job_id

    cur.execute("SELECT honker_ack(%s, %s)", [job_id, "w1"])
    assert cur.fetchone()[0] == 1, "ack must match the claim"

print("PASS django")
