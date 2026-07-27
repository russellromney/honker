"""The README quickstart, start to finish, in one process.

The shortest complete honker loop: a business write and a job enqueue
commit in the same transaction, then a worker claims that job and acks
it. Everything the other examples build on, with nothing else layered
on top.

`atomic.py` goes deeper on the transaction (it also shows rollback);
`worker.py` goes deeper on the worker loop (retries, dead-lettering).

    PYTHONPATH=packages .venv/bin/python packages/honker/examples/quickstart.py
"""

import asyncio
import os
import tempfile

import honker


async def main():
    with tempfile.TemporaryDirectory() as d:
        db = honker.open(os.path.join(d, "app.db"))
        emails = db.queue("emails")

        # A plain business table. Nothing honker-specific.
        with db.transaction() as tx:
            tx.execute(
                "CREATE TABLE orders "
                "(id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"
            )

        # The order INSERT and the job enqueue commit together. A worker
        # in another process wakes when this transaction commits.
        with db.transaction() as tx:
            tx.execute(
                "INSERT INTO orders (user_id, amount) VALUES (?, ?)", [42, 19.99]
            )
            emails.enqueue(
                {"to": "alice@example.com", "body": "Receipt for 19.99"}, tx=tx
            )

        async def worker():
            async for job in emails.claim("w1"):
                print(f"sending email to {job.payload['to']}: {job.payload['body']}")
                assert job.ack()
                return

        await asyncio.wait_for(worker(), timeout=5.0)
        print("done")


if __name__ == "__main__":
    asyncio.run(main())
