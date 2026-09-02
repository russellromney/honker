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
not perform runtime schema validation. Anything that can write to the queue —
another process, another language binding, or a raw `honker_enqueue` on your own
SQLite connection — decides what actually comes back, so validate at the boundary
when the producer is not under your control.

Every job carries the same twelve fields, whether it came from a claim or from
`getJob()`:

| field | |
| --- | --- |
| `id` `queue` `payload` | identity and body; `payload` is already JSON-decoded |
| `state` | `"pending"` or `"processing"` |
| `priority` `attempts` `maxAttempts` | scheduling and retry budget |
| `runAt` `createdAt` `claimExpiresAt` `expiresAt` | **Unix epoch seconds, not milliseconds** |

`workerId`, `claimExpiresAt`, and `expiresAt` are `null` when they do not apply
(an unclaimed job, a job with no TTL). On a claimed job `workerId` and
`claimExpiresAt` are always set. For a `Date`, multiply: `new Date(job.createdAt * 1000)`.

### Upgrading `getJob()` from 0.5.1

`getJob()` used to hand back the SQL layer's raw row. It now returns the decoded
camelCase snapshot above. Two things change for existing callers:

- `row.run_at` is now `job.runAt`, and so on for every snake_case field. The old
  names read back as `undefined` rather than throwing.
- `payload` is already decoded. Drop any `JSON.parse(row.payload)` around it.

`getJob()` is also scoped to its own queue now: it returns `null` for a job owned
by a different queue, where it previously returned that queue's row. Job ids are
globally unique, so the old lookup could hand `emails` an SMS payload. There is
no unscoped replacement yet (tracked in issue #134) — until then, call the SQL
function directly on your own connection with `SELECT honker_get_job(?)`, which
still returns the raw snake_case row. `cancel()` is unchanged and stays global.

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
