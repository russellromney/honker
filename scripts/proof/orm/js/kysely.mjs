// Kysely, exactly as guides/orm/javascript.mdx shows it.
import assert from 'node:assert/strict';
import Database from 'better-sqlite3';
import { Kysely, SqliteDialect, sql } from 'kysely';
import { extensionPath } from '@russellthehippo/honker-node/extension';

const sqlite = new Database(':memory:');
sqlite.loadExtension(extensionPath());
sqlite.prepare('SELECT honker_bootstrap()').run();
const db = new Kysely({ dialect: new SqliteDialect({ database: sqlite }) });

const payload = JSON.stringify({ to: 'alice@example.com' });
const enqueued = await sql`
  SELECT honker_enqueue(${'emails'}, ${payload}, NULL, ${null}, ${0}, ${3}, NULL) AS id
`.execute(db);
const id = enqueued.rows[0].id;
assert.ok(id > 0, `expected a job id, got ${id}`);

const claimRes = await sql`
  SELECT honker_claim_batch(${'emails'}, ${'w1'}, ${8}, ${300}) AS jobs
`.execute(db);
const claimed = JSON.parse(claimRes.rows[0].jobs);
assert.equal(claimed.length, 1);
assert.equal(claimed[0].id, id);

const ackRes = await sql`SELECT honker_ack(${id}, ${'w1'}) AS ok`.execute(db);
assert.equal(ackRes.rows[0].ok, 1);
await db.destroy();
console.log('PASS kysely');
