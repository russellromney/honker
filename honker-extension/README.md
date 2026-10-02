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
