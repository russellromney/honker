// Kysely, exactly as guides/orm/javascript.mdx shows it.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { Kysely, SqliteDialect, sql } from 'kysely';
import { extensionPath } from '@russellthehippo/honker-node/extension';
import { run } from './surface.mjs';

const dbPath = process.env.HONKER_TEST_DB;
if (!dbPath) throw new Error('HONKER_TEST_DB is required');
fs.rmSync(dbPath, { force: true });

const sqlite = new Database(dbPath);
sqlite.loadExtension(extensionPath());
sqlite.prepare('SELECT honker_bootstrap()').run();
const db = new Kysely({ dialect: new SqliteDialect({ database: sqlite }) });

function bound(query, args) {
  const parts = query.split('?');
  if (parts.length - 1 !== args.length) {
    throw new Error(`bind mismatch: ${parts.length - 1} placeholders, ${args.length} args`);
  }
  let frag = sql.raw(parts[0]);
  for (let i = 0; i < args.length; i++) {
    frag = sql`${frag}${args[i]}${sql.raw(parts[i + 1])}`;
  }
  return frag;
}

await run(async (query, args) => {
  const { rows } = await bound(query, args).execute(db);
  return rows[0] ? Object.values(rows[0])[0] : null;
}, 'ky');

await sql`CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)`.execute(db);

const committed = await db.transaction().execute(async (tx) => {
  await sql`INSERT INTO orders (id, user_id) VALUES (${42}, ${1})`.execute(tx);
  const { rows } = await sql`
    SELECT honker_enqueue(${'ky_atomic'}, ${JSON.stringify({ order_id: 42 })}, NULL, ${null}, ${0}, ${3}, NULL) AS id
  `.execute(tx);
  return rows[0].id;
});
const committedCount = await sql`SELECT COUNT(*) AS n FROM orders WHERE id = ${42}`.execute(db);
assert.equal(committedCount.rows[0].n, 1);
const committedJob = await sql`SELECT honker_get_job(${committed}) AS job`.execute(db);
assert.match(String(committedJob.rows[0].job), /order_id/);

let rolled;
try {
  await db.transaction().execute(async (tx) => {
    await sql`INSERT INTO orders (id, user_id) VALUES (${43}, ${1})`.execute(tx);
    const { rows } = await sql`
      SELECT honker_enqueue(${'ky_atomic'}, ${JSON.stringify({ order_id: 43 })}, NULL, ${null}, ${0}, ${3}, NULL) AS id
    `.execute(tx);
    rolled = rows[0].id;
    throw new Error('rollback');
  });
} catch (err) {
  if (err.message !== 'rollback') throw err;
}
const rolledCount = await sql`SELECT COUNT(*) AS n FROM orders WHERE id = ${43}`.execute(db);
assert.equal(rolledCount.rows[0].n, 0);
const rolledJob = await sql`SELECT honker_get_job(${rolled}) AS job`.execute(db);
assert.equal(rolledJob.rows[0].job, '');

await db.destroy();
console.log('PASS kysely');
