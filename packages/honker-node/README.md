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

For streams, notify/listen, SQL functions, and full scheduler docs, see the main repo and docs site.
