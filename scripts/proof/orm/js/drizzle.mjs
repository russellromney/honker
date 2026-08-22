// Drizzle ORM, exactly as guides/orm/javascript.mdx shows it: the
// sql`` template with interpolated values, which is where Drizzle's
// own parameter binding sits.
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { sql } from 'drizzle-orm';
import { extensionPath } from '@russellthehippo/honker-node/extension';

const sqlite = new Database(':memory:');
sqlite.loadExtension(extensionPath());
sqlite.prepare('SELECT honker_bootstrap()').run();
const db = drizzle(sqlite);

const payload = JSON.stringify({ to: 'alice@example.com' });
const { id } = db.get(sql`
  SELECT honker_enqueue(${'emails'}, ${payload}, NULL, ${null}, ${0}, ${3}, NULL) AS id
`);
assert.ok(id > 0, `expected a job id, got ${id}`);

const claimed = JSON.parse(
  db.get(sql`SELECT honker_claim_batch(${'emails'}, ${'w1'}, ${8}, ${300}) AS jobs`).jobs,
);
assert.equal(claimed.length, 1);
assert.equal(claimed[0].id, id);

assert.equal(db.get(sql`SELECT honker_ack(${id}, ${'w1'}) AS ok`).ok, 1);
sqlite.close();
console.log('PASS drizzle');
