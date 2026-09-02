# honker-bun

Bun binding for [Honker](https://github.com/russellromney/honker): durable queues, streams, pub/sub, and time-trigger scheduling on SQLite.

Full docs:

- [Main repo](https://github.com/russellromney/honker)
- [Docs](https://honker.dev)

## Install

```bash
bun add @russellthehippo/honker-bun
```

You also need the Honker SQLite extension from the main repo.

## Watcher backends

`open(path, extPath, { watcherBackend: "polling" })` accepts the
default polling backend aliases (`"polling"` / `"poll"`). Experimental
`"kernel"` / `"shm"` requests route through `honker-core` via the loaded
Honker extension and fail loudly if that extension was not built with
the matching feature.

## Quick start

```ts
import { open } from "@russellthehippo/honker-bun";

const db = open("app.db", "./libhonker_ext.dylib");
const q = db.queue("emails");

q.enqueue({ to: "alice@example.com" });

for await (const job of q.claim("worker-1")) {
  sendEmail(job.payload);
  job.ack();
}
```

Delayed jobs use `runAt`:

```ts
q.enqueue({ to: "later@example.com" }, { runAt: Math.floor(Date.now() / 1000) + 10 });
```

Give a queue a payload contract and claimed jobs keep it. A `getJob()`
snapshot carries every field but hands the payload back as raw JSON
text, so you parse it yourself:

```ts
interface EmailPayload {
  to: string;
  template: "welcome" | "receipt";
}

const emails = db.queue<EmailPayload>("emails");
const id = emails.enqueue({ to: "alice@example.com", template: "welcome" });

// Read-only snapshot: data, no claim methods. Its payload is raw JSON
// text — parse it yourself.
const pending = emails.getJob(id);
console.log(pending?.state, pending?.priority, pending?.runAt);
const payload = pending ? (JSON.parse(pending.payload) as EmailPayload) : null;

const job = emails.claimOne("worker-1");
if (job) {
  console.log(job.payload.template, `${job.attempts}/${job.maxAttempts}`);
  job.ack();
}
```

Payload generics describe the expected JSON shape at compile time.
Honker never validates payload shape — not in this binding, not in the
database — so every process writing to a queue has to agree on the type.

Claimed jobs and snapshots carry every field the core returns: `id`,
`queue`, `payload`, `state`, `priority`, `runAt`, `workerId`,
`claimExpiresAt`, `attempts`, `maxAttempts`, `createdAt`, `expiresAt`.
A claimed `Job` also has `ack`, `retry`, `fail`, and `heartbeat`; a
`JobSnapshot` is data only, because reading a row does not claim it.

`getJob()` looks a job up by id across every queue, not just the one you
called it on — job ids are globally unique. Check `snapshot.queue` if
that matters to you. (The Node binding scopes its `getJob`; #134 tracks
making the rest of the bindings agree.)

Payload encoding differs between the two, and that is deliberate: a
claimed `job.payload` is decoded (it always was), while a snapshot's
`payload` stays raw JSON text (it always was). Bindings currently
disagree here — Node decodes snapshot payloads, Bun, Go, and Python do
not — and settling on one convention is a separate decision, not part of
the job-detail work.

Recurring schedules use `schedule`:

```ts
const sched = db.scheduler();
sched.add("fast", { queue: "emails", schedule: "@every 1s", payload: { kind: "tick" } });
```

Supported schedule forms:

- `0 3 * * *`
- `*/2 * * * * *`
- `@every 1s`

For full API docs and SQL details, see the main repo and docs site.
