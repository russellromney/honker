// better-sqlite3, exactly as guides/orm/javascript.mdx shows it.
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { extensionPath, extensionInfo } from '@russellthehippo/honker-node/extension';

const info = extensionInfo();
assert.equal(info.entrypoint, 'sqlite3_honkerext_init');
assert.match(
  info.path,
  new RegExp(`honker-ext-${process.env.HONKER_PLATFORM}`),
  `expected the packaged extension, resolved ${info.path}`,
);

const db = new Database(':memory:');
db.loadExtension(extensionPath());
db.prepare('SELECT honker_bootstrap()').run();

// Every numeric argument bound, never a SQL literal. better-sqlite3
// binds JS numbers as REAL; literals would sidestep that entirely.
const payload = JSON.stringify({ to: 'alice@example.com' });
const { id } = db
  .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
  .get('emails', payload, 0, 3);
assert.ok(id > 0, `expected a job id, got ${id}`);

const claimed = JSON.parse(
  db.prepare('SELECT honker_claim_batch(?, ?, ?, ?) AS jobs').get('emails', 'w1', 8, 300).jobs,
);
assert.equal(claimed.length, 1);
assert.equal(claimed[0].id, id);
assert.equal(JSON.parse(claimed[0].payload).to, 'alice@example.com');

assert.equal(db.prepare('SELECT honker_ack(?, ?) AS ok').get(id, 'w1').ok, 1);
db.close();
console.log('PASS better-sqlite3');
