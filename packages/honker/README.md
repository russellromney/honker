# honker (Python)

Python binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs live in the main repo and docs site:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
pip install honker
```

The Python wheel includes Honker's SQLite loadable extension when the
package is built through the release/proof workflow. To load Honker into
your own `sqlite3` or ORM connection:

```python
import honker
import sqlite3

conn = sqlite3.connect("app.db")
honker.load_extension(conn)
conn.execute("SELECT honker_bootstrap()")
```

If a client needs the raw pieces instead, use `extension_info()`:

```python
path, entrypoint = honker.extension_info()
```

For local source builds, build and copy the extension before building
the wheel:

```bash
git clone https://github.com/russellromney/honker
cd honker
cargo build --release -p honker-extension
scripts/copy-python-extension.sh
```

## Quick start

```python
import honker

db = honker.open("app.db")
q = db.queue("emails")

q.enqueue({"to": "alice@example.com"})

async for job in q.claim("worker-1"):
    send_email(job.payload)
    job.ack()
```

Delayed jobs use `run_at`:

```python
import time

q.enqueue({"to": "later@example.com"}, run_at=int(time.time()) + 10)
```

Recurring schedules use `schedule` expressions:

```python
sched = db.scheduler()
sched.add("fast", queue="emails", schedule="@every 1s", payload={"kind": "tick"})
```

Supported schedule forms:

- 5-field cron: `0 3 * * *`
- 6-field cron: `*/2 * * * * *`
- interval: `@every 1s`

## Notes

- `claim()` wakes on database updates and on due deadlines like `run_at`.
- `schedule` is the canonical recurring-schedule name.
- `cron` still works as a compatibility alias in older call sites.

For streams, notify/listen, tasks, SQL extension usage, and full scheduler docs, see the main repo and docs site.
