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

Recurring schedules are registered on a `Scheduler`. Build the schedule with
`crontab(expr)` or `every_s(n)`, then run the scheduler loop (it enqueues onto
the target queue on each boundary; a normal worker claims the jobs):

```python
from honker import Scheduler, crontab, every_s

sched = Scheduler(db)
sched.add(name="fast", queue="emails", schedule=every_s(1), payload={"kind": "tick"})
sched.add(name="nightly", queue="emails", schedule=crontab("0 3 * * *"))

await sched.run()  # acquires the leader lock and fires due tasks
```

`add()` is keyword-only and `schedule` must be a `CronSchedule` (from `crontab`
or `every_s`), not a raw string. Supported schedule expressions:

- 5-field cron: `crontab("0 3 * * *")`
- 6-field cron: `crontab("*/2 * * * * *")`
- interval: `every_s(1)` (use `every_s`, not `crontab("@every …")`)

## Notes

- `claim()` wakes on database updates and on due deadlines like `run_at`.
- `schedule` is the canonical recurring-schedule name.
- `cron` still works as a compatibility alias in older call sites.
- Construct `db.queue(name)` handles **outside** an open `db.transaction()`.
  First open for a name runs schema init in its own write transaction; doing
  that inside an outer `transaction()` raises `RuntimeError` (would otherwise
  deadlock the single writer). Create the handle first, then pass `tx=` to
  `enqueue`.

For streams, notify/listen, tasks, SQL extension usage, and full scheduler docs, see the main repo and docs site.
