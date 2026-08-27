# @russellthehippo/honker-node

Node.js binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs live here:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
npm install @russellthehippo/honker-node
```

That's everything for `honker.open()`. The native binding is
statically linked; there is no separate extension to install.

## Using Honker with an ORM

If you'd rather load Honker onto a connection you already own — a
Drizzle, Kysely, or plain better-sqlite3 handle — you need the SQLite
loadable extension instead. It installs automatically as an optional
dependency, and this tells you where it is:

```js
const Database = require("better-sqlite3");
const { extensionPath } = require("@russellthehippo/honker-node/extension");

const db = new Database("app.db");
db.loadExtension(extensionPath());
db.prepare("SELECT honker_bootstrap()").run();
```

Now `honker_enqueue()` runs inside your own transactions, which is the
point — enqueueing outside them loses atomicity.

Set `HONKER_EXTENSION_PATH` to override. Prebuilt for macOS
(arm64, x64) and Linux (x64, arm64, glibc); on other platforms build it
with `cargo build --release -p honker-extension` and point
`HONKER_EXTENSION_PATH` at the result.

## Quick start

```js
const honker = require("@russellthehippo/honker-node");

const db = honker.open("app.db");
const q = db.queue("emails");

q.enqueue({ to: "alice@example.com" });

for await (const job of q.claim("worker-1")) {
  sendEmail(job.payload);
  job.ack();
}
```

Delayed jobs use `runAt`:

```js
q.enqueue({ to: "later@example.com" }, { runAt: Math.floor(Date.now() / 1000) + 10 });
```

TypeScript callers can give a queue a payload contract. Claimed jobs and
`getJob()` snapshots preserve it:

```ts
interface EmailPayload {
  to: string;
  template: "welcome" | "receipt";
}

const emails = db.queue<EmailPayload>("emails");
emails.enqueue({ to: "alice@example.com", template: "welcome" });

const job = emails.claimOne("worker-1");
if (job) {
  console.log(job.payload.template, job.priority, job.runAt, job.createdAt);
  job.ack();
}
```

Payload generics describe the expected JSON shape at compile time; Honker does
not perform runtime schema validation.

Queue lifecycle events are an opt-in, bounded observability feed. Configuration
is stored in the database so producers and workers in other processes use the
same behavior. Existing connections refresh configuration within 100 ms; the
connection that calls `configureQueueEvents()` sees it immediately:

```ts
db.configureQueueEvents({ maxEvents: 10_000, includePayload: false });

const events = db.queueEvents({ queue: "emails", fromOffset: 0 });
for await (const event of events) {
  console.log(event.offset, event.type, event.jobId);
}
```

Events are appended in the same SQLite transaction as successful queue state
transitions. They are intended for dashboards, metrics, and debugging—not as a
permanent audit log or an exactly-once business event bus. Save the last offset
to replay retained events after reconnecting. The internal
`_honker:queue-events:v1` stream topic is reserved and cannot be published via
`db.stream()`.

Recurring schedules use `schedule`:

```js
const sched = db.scheduler();
sched.add("fast", { queue: "emails", schedule: "@every 1s", payload: { kind: "tick" } });
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

## Notes

- `claim()` wakes on database updates and on due deadlines.
- `schedule` is the canonical recurring-schedule option.
- `cron` still works as a compatibility alias.

### Upgrading stream checkpoints written by 0.4.6

Node 0.4.6 swapped the stream topic and consumer name at the SQL boundary in
`stream.saveOffset(consumer, offset)`, `stream.saveOffsetTx(...)`, and
`stream.getOffset(consumer)`. Publishing and explicit-offset `readSince()` were
unaffected. Named-consumer `readFromConsumer()` and `subscribe()` were
self-consistent within Node 0.4.6, but could not share resume positions with
Python or another binding.

Later versions use the canonical `(consumer, topic)` key and automatically
migrate a 0.4.6 checkpoint the first time that stream/consumer pair is read or
saved. Migration is transactional, preserves the old row, and verifies that
the saved offset belongs to a retained event in the requested stream. A
canonical row always wins when both key orders exist.

Node 0.5.0's alpha compatibility path deliberately rejects checkpoint state it cannot
verify—for example, an arbitrary offset or one whose event was manually
deleted—with `CheckpointMigrationError` and code
`HONKER_CHECKPOINT_MIGRATION_UNVERIFIABLE`. Guarded reads (`getOffset()`,
`readFromConsumer()`, and `subscribe()`) throw because guessing could skip
events. Recover by explicitly establishing canonical progress:

```js
stream.saveOffset("worker-c", 0);           // replay retained events
stream.saveOffset("worker-c", knownOffset); // resume after a known event
```

`saveOffsetTx()` supports the same recovery inside a caller-owned transaction.
Running 0.4.6 and a corrected version against the same consumer concurrently
is unsupported during the upgrade.

For streams, notify/listen, SQL functions, and full scheduler docs, see the main repo and docs site.
