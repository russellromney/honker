// better-sqlite3, exactly as guides/orm/javascript.mdx shows it.
// docs:start example
import assert from 'node:assert/strict';
import fs from 'node:fs';
import Database from 'better-sqlite3';
import { extensionPath, extensionInfo } from '@russellthehippo/honker-node/extension';
import { run } from './surface.mjs';

const info = extensionInfo();
assert.equal(info.entrypoint, 'sqlite3_honkerext_init');
assert.match(
  info.path,
  new RegExp(`honker-ext-${process.env.HONKER_PLATFORM}`),
  `expected the packaged extension, resolved ${info.path}`,
);

const dbPath = process.env.HONKER_TEST_DB;
if (!dbPath) throw new Error('HONKER_TEST_DB is required');
fs.rmSync(dbPath, { force: true });

const db = new Database(dbPath);
db.loadExtension(extensionPath());
db.prepare('SELECT honker_bootstrap()').run();

await run((sql, args) => {
  const row = db.prepare(sql).get(...args);
  return row ? Object.values(row)[0] : null;
}, 'b3');

db.exec('CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)');
const committed = db.transaction(() => {
  db.prepare('INSERT INTO orders (id, user_id) VALUES (?, ?)').run(42, 1);
  return db
    .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
    .get('b3_atomic', JSON.stringify({ order_id: 42 }), 0, 3).id;
})();
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM orders WHERE id = 42').get().n, 1);
assert.match(db.prepare('SELECT honker_get_job(?) AS job').get(committed).job, /order_id/);

let rolled;
try {
  db.transaction(() => {
    db.prepare('INSERT INTO orders (id, user_id) VALUES (?, ?)').run(43, 1);
    rolled = db
      .prepare('SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id')
      .get('b3_atomic', JSON.stringify({ order_id: 43 }), 0, 3).id;
    throw new Error('rollback');
  })();
} catch (err) {
  if (err.message !== 'rollback') throw err;
}
assert.equal(db.prepare('SELECT COUNT(*) AS n FROM orders WHERE id = 43').get().n, 0);
assert.equal(db.prepare('SELECT honker_get_job(?) AS job').get(rolled).job, '');

db.close();
console.log('PASS better-sqlite3');
// docs:end example
