# honker-extension

SQLite loadable extension for [Honker](https://honker.dev). Adds every `honker_*` SQL scalar function (queues, streams, scheduler, pub/sub, rate limits, locks, results) to any SQLite 3.9+ client.

## Install

From crates.io (builds `libhonker_ext.dylib` / `.so` for your platform):

```bash
cargo install honker-extension
# or build from source:
cargo build --release -p honker-extension
# → target/release/libhonker_ext.{dylib,so}
```

Prebuilt binaries per platform are available at [GitHub releases](https://github.com/russellromney/honker/releases/latest).

## Use

```sql
.load ./libhonker_ext
SELECT honker_bootstrap();

-- Queues
SELECT honker_enqueue('emails', '{"to":"alice"}', NULL, NULL, 0, 3, NULL);
SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
SELECT honker_ack_batch('[1,2,3]', 'worker-1');

-- Streams (durable pub/sub)
SELECT honker_stream_publish('orders', 'k', '{"id":42}');
SELECT honker_stream_read_since('orders', 0, 1000);

-- pg_notify-style pub/sub
SELECT notify('orders', '{"id":42}');
```

Full SQL reference: [honker.dev/reference/extension](https://honker.dev/reference/extension/).

## License

Apache-2.0.

### Fenced completion (attempt token)

`honker_claim_batch` returns each job's `attempts`. That value is the
claim's fencing token: pass it as the last argument to finish the job.

```sql
SELECT honker_ack(7, 'worker-1', 3);                        -- 1 = done
SELECT honker_retry(7, 'worker-1', 30, 'timeout', 3);       -- 1 = retried or dead
SELECT honker_fail(7, 'worker-1', 'rejected', 3);           -- 1 = moved to dead
SELECT honker_heartbeat(7, 'worker-1', 300, 3);             -- 1 = lease extended
SELECT honker_ack_batch('[[7,3],[8,1]]', 'worker-1');       -- [id, attempt] pairs
```

A fenced call acts only if the row is still that claim: same id, worker
id and `attempts`, and still `processing`. It does not check the lease.
A reclaim increases `attempts`, and dead-lettering, expiry and cancel
remove the row, so:

- A stale handler gets 0, even when a restarted worker with the same
  worker id has reclaimed the job.
- A handler that overran its lease still completes if nobody reclaimed
  the job. The job does not run again. A fenced heartbeat in that state
  sets `claim_expires_at = now + extend_s` again.

The shorter forms (`honker_ack(id, worker_id)`, `honker_retry(id,
worker_id, delay_s, error)`, `honker_fail(id, worker_id, error)`,
`honker_heartbeat(id, worker_id, extend_s)`, and plain ids in
`honker_ack_batch`) are unchanged and **unfenced**. They check the worker
id and an unexpired lease. A stale handler that shares the new holder's
worker id passes that check and can ack, retry or fail the newer attempt
(issue #176). Use the fenced forms when worker ids can repeat, for
example a worker restarted with a fixed id while its old process still
runs.

### SQL call context

`honker_claim_batch`, `honker_fail`, `honker_sweep_expired`, and a
`honker_retry` that moves the job to `_honker_dead` (its attempts are used
up) must run as a **separate SELECT**, after any write cursors on that
connection are finished. Do not call them inside a trigger, an
INSERT/UPDATE/DELETE, or a RETURNING expression. An unfinished
`INSERT ... RETURNING` cursor also blocks them, even when the call itself
is a separate SELECT.

These operations use a savepoint so that a failure partway through cannot
lose the job. SQLite cannot open a savepoint while a write statement is
active, and the error says so. A larger `busy_timeout` does not help:
finish or close the write cursor. A `honker_retry` that puts the job back
to pending is a single UPDATE with no savepoint, so it works in those
contexts. The branch depends on the job's attempts, so if a call might be
the final attempt, run `honker_retry` as a separate SELECT too.

An explicit application transaction is supported and keeps all work
atomic. Use `BEGIN IMMEDIATE`: it takes the write lock up front, so other
connections' commits cannot fail the transaction partway through.

```sql
BEGIN IMMEDIATE;
UPDATE app_orders SET status = 'failed' WHERE id = 42;
-- If using RETURNING above, finish its cursor before the next statement.
SELECT honker_fail(7, 'worker-1', 'delivery rejected');
COMMIT;
```

The application must roll back the transaction on an error. Versions
before savepoint-protected job transitions accepted these calls inside DML;
keep them as separate statements instead.
