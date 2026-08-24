// Drizzle ORM, exactly as guides/orm/javascript.mdx shows it.
import assert from 'node:assert/strict';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { drizzle } from 'drizzle-orm/better-sqlite3';
import { sql } from 'drizzle-orm';
import { extensionPath } from '@russellthehippo/honker-node/extension';
import { run } from './surface.mjs';

const dbPath = process.env.HONKER_TEST_DB;
if (!dbPath) throw new Error('HONKER_TEST_DB is required');
fs.rmSync(dbPath, { force: true });

const sqlite = new Database(dbPath);
sqlite.loadExtension(extensionPath());
sqlite.prepare('SELECT honker_bootstrap()').run();
const db = drizzle(sqlite);

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

await run((query, args) => {
  const row = db.get(bound(query, args));
  return row ? Object.values(row)[0] : null;
}, 'dr');

db.run(sql.raw('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)'));
const committed = db.transaction((tx) => {
  tx.run(sql`INSERT INTO orders (id, user_id) VALUES (${42}, ${1})`);
  return tx.get(sql`
    SELECT honker_enqueue(${'dr_atomic'}, ${JSON.stringify({ order_id: 42 })}, NULL, ${null}, ${0}, ${3}, NULL) AS id
  `).id;
});
assert.equal(db.get(sql`SELECT COUNT(*) AS n FROM orders WHERE id = ${42}`).n, 1);
assert.match(db.get(sql`SELECT honker_get_job(${committed}) AS job`).job, /order_id/);

let rolled;
try {
  db.transaction((tx) => {
    tx.run(sql`INSERT INTO orders (id, user_id) VALUES (${43}, ${1})`);
    rolled = tx.get(sql`
      SELECT honker_enqueue(${'dr_atomic'}, ${JSON.stringify({ order_id: 43 })}, NULL, ${null}, ${0}, ${3}, NULL) AS id
    `).id;
    throw new Error('rollback');
  });
} catch (err) {
  if (err.message !== 'rollback') throw err;
}
assert.equal(db.get(sql`SELECT COUNT(*) AS n FROM orders WHERE id = ${43}`).n, 0);
assert.equal(db.get(sql`SELECT honker_get_job(${rolled}) AS job`).job, '');

sqlite.close();
console.log('PASS drizzle');
