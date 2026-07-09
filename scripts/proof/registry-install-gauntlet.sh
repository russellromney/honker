#!/usr/bin/env bash
set -euo pipefail

# Install Honker bindings from their public package/source locations into
# throwaway consumer projects, then run user-shaped flows against them.
#
# Package bindings are installed from registries. Source-distributed bindings
# are downloaded through their public module/archive sources, not imported from
# this checkout. The SQLite extension path used by extension-loading bindings is
# discovered from the PyPI wheel because that wheel is a published distribution
# artifact with the extension bundled.

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TMP="${TMPDIR:-/tmp}/honker-registry-gauntlet-$$"
PYTHON_BIN="${PYTHON_BIN:-python3}"
PYTHON_VERSION="${PYTHON_VERSION:-3.14}"
RUBY_BIN="${RUBY_BIN:-}"
if [[ -z "$RUBY_BIN" ]] && command -v brew >/dev/null 2>&1; then
  BREW_RUBY="$(brew --prefix ruby 2>/dev/null || true)"
  if [[ -n "$BREW_RUBY" && -x "$BREW_RUBY/bin/ruby" ]]; then
    RUBY_BIN="$BREW_RUBY/bin/ruby"
  fi
fi
RUBY_BIN="${RUBY_BIN:-ruby}"

HONKER_PY_VERSION="${HONKER_PY_VERSION:-}"
HONKER_NODE_VERSION="${HONKER_NODE_VERSION:-}"
HONKER_BUN_VERSION="${HONKER_BUN_VERSION:-}"
HONKER_RUBY_VERSION="${HONKER_RUBY_VERSION:-}"
HONKER_DOTNET_VERSION="${HONKER_DOTNET_VERSION:-}"
HONKER_ELIXIR_VERSION="${HONKER_ELIXIR_VERSION:-}"
HONKER_RUST_VERSION="${HONKER_RUST_VERSION:-}"
HONKER_GO_REF="${HONKER_GO_REF:-latest}"
HONKER_CPP_REF="${HONKER_CPP_REF:-main}"

mkdir -p "$TMP"
trap 'rm -rf "$TMP"' EXIT

pkg_spec() {
  local name="$1"
  local version="$2"
  if [[ -n "$version" ]]; then
    printf '%s==%s' "$name" "$version"
  else
    printf '%s' "$name"
  fi
}

npm_spec() {
  local name="$1"
  local version="$2"
  if [[ -n "$version" ]]; then
    printf '%s@%s' "$name" "$version"
  else
    printf '%s' "$name"
  fi
}

echo "== python registry gauntlet =="
uv venv --python "$PYTHON_VERSION" "$TMP/python"
uv pip install --python "$TMP/python/bin/python" "$(pkg_spec honker "$HONKER_PY_VERSION")"
"$TMP/python/bin/python" <<'PY' >"$TMP/extension-path"
import asyncio
import json
import os
import tempfile
import honker

async def main():
    with tempfile.TemporaryDirectory(prefix="honker-py-gauntlet-") as d:
        db = honker.open(os.path.join(d, "app.db"))
        q = db.queue("emails")
        job_id = q.enqueue({"to": "alice@example.com"})
        job = q.claim_one("worker-1")
        assert job and job.id == job_id
        assert job.payload["to"] == "alice@example.com"
        assert job.ack()

        q2 = db.queue("orders")
        with db.transaction() as tx:
            tx.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
            tx.execute("INSERT INTO orders VALUES (?, ?)", [1, 100])
            q2.enqueue({"order_id": 1}, tx=tx)
        assert db.query("SELECT COUNT(*) AS c FROM orders")[0]["c"] == 1
        assert q2.claim_one("worker-2").payload["order_id"] == 1

        try:
            with db.transaction() as tx:
                tx.execute("INSERT INTO orders VALUES (?, ?)", [2, 200])
                q2.enqueue({"order_id": 2}, tx=tx)
                raise RuntimeError("rollback-me")
        except RuntimeError:
            pass
        assert db.query("SELECT COUNT(*) AS c FROM orders WHERE id=2")[0]["c"] == 0

        stream = db.stream("events")
        stream.publish({"id": 1})
        event = await asyncio.wait_for(anext(stream.subscribe(from_offset=0)), 2)
        assert event.payload == {"id": 1}
        stream.save_offset("billing", event.offset)
        assert stream.get_offset("billing") == event.offset

        listener = db.listen("orders", fallback_poll_s=0.05)
        with db.transaction() as tx:
            tx.query("SELECT notify(?, ?)", ["orders", json.dumps({"id": 1})])
        note = await asyncio.wait_for(listener.__anext__(), 2)
        assert note.payload == {"id": 1}

        path, _entrypoint = honker.extension_info()
        print(path)

asyncio.run(main())
PY
EXT_PATH="$(tail -n 1 "$TMP/extension-path")"
if [[ ! -f "$EXT_PATH" ]]; then
  echo "published Python package did not expose extension at $EXT_PATH" >&2
  exit 1
fi

echo "== node registry gauntlet =="
mkdir -p "$TMP/node"
(
  cd "$TMP/node"
  npm init -y >/dev/null
  npm install "$(npm_spec @russellthehippo/honker-node "$HONKER_NODE_VERSION")" >/dev/null
  node <<'JS'
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { open } = require('@russellthehippo/honker-node');

const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'honker-node-registry-'));
try {
  const db = open(path.join(dir, 'app.db'));
  const q = db.queue('emails');
  const id = q.enqueue({ to: 'alice@example.com' });
  const job = q.claimOne('worker-1');
  assert.equal(job.id, id);
  assert.equal(job.payload.to, 'alice@example.com');
  assert.equal(job.ack(), true);

  db.query('CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)');
  const orders = db.queue('orders');
  let tx = db.transaction();
  tx.execute('INSERT INTO orders VALUES (?, ?)', [1, 100]);
  orders.enqueue({ orderId: 1 }, { tx });
  tx.rollback();
  assert.equal(db.query('SELECT COUNT(*) AS c FROM orders')[0].c, 0);
  assert.equal(orders.claimOne('w'), null);

  tx = db.transaction();
  tx.execute('INSERT INTO orders VALUES (?, ?)', [2, 200]);
  orders.enqueue({ orderId: 2 }, { tx });
  tx.commit();
  assert.equal(db.query('SELECT COUNT(*) AS c FROM orders')[0].c, 1);
  assert.equal(orders.claimOne('w').payload.orderId, 2);

  const stream = db.stream('events');
  const off = stream.publish({ id: 1 });
  const events = stream.readSince(0, 10);
  assert.equal(events.length, 1);
  assert.equal(events[0].offset, off);
  assert.equal(events[0].payload.id, 1);
  assert.equal(stream.saveOffset('billing', off), true);
  assert.equal(stream.getOffset('billing'), off);

  db.notify('orders', { id: 1 });
  assert.equal(db.query("SELECT COUNT(*) AS c FROM _honker_notifications WHERE channel='orders'")[0].c, 1);
  db.close();
  console.log('node registry gauntlet ok');
} finally {
  fs.rmSync(dir, { recursive: true, force: true });
}
JS
)

echo "== bun registry gauntlet =="
mkdir -p "$TMP/bun"
(
  cd "$TMP/bun"
  bun init -y >/dev/null
  bun add "$(npm_spec @russellthehippo/honker-bun "$HONKER_BUN_VERSION")" typescript >/dev/null
  cat > gauntlet.ts <<'TS'
import { strict as assert } from "node:assert";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { open } from "@russellthehippo/honker-bun";

const dir = mkdtempSync(join(tmpdir(), "honker-bun-registry-"));
try {
  const db = open(join(dir, "app.db"), process.env.HONKER_EXT_PATH!);
  const q = db.queue("emails");
  const id = q.enqueue({ to: "alice@example.com" });
  const job = q.claimOne("worker-1")!;
  assert.equal(job.id, id);
  assert.equal((job.payload as any).to, "alice@example.com");
  assert.equal(job.ack(), true);

  db.raw.exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)");
  const orders = db.queue("orders");
  let tx = db.transaction();
  tx.execute("INSERT INTO orders VALUES (?, ?)", [1, 100]);
  orders.enqueue({ orderId: 1 }, { tx });
  tx.rollback();
  assert.equal(db.raw.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM orders").get()!.c, 0);

  tx = db.transaction();
  tx.execute("INSERT INTO orders VALUES (?, ?)", [2, 200]);
  orders.enqueue({ orderId: 2 }, { tx });
  tx.commit();
  assert.equal(orders.claimOne("w")!.payload.orderId, 2);

  const stream = db.stream("events");
  const off = stream.publish({ id: 1 });
  const events = stream.readSince(0, 10);
  assert.equal(events[0].offset, off);
  assert.equal((events[0].payload as any).id, 1);
  assert.equal(stream.saveOffset("billing", off), true);
  assert.equal(stream.getOffset("billing"), off);

  db.notify("orders", { id: 1 });
  assert.equal(db.raw.query<{ c: number }, []>("SELECT COUNT(*) AS c FROM _honker_notifications WHERE channel='orders'").get()!.c, 1);
  db.close();
  console.log("bun registry gauntlet ok");
} finally {
  rmSync(dir, { recursive: true, force: true });
}
TS
  HONKER_EXT_PATH="$EXT_PATH" bun run gauntlet.ts
)

echo "== ruby registry gauntlet =="
GEM_HOME="$TMP/ruby-gems"
GEM_PATH="$TMP/ruby-gems"
export GEM_HOME GEM_PATH
if [[ -n "$HONKER_RUBY_VERSION" ]]; then
  "$RUBY_BIN" -S gem install honker -v "$HONKER_RUBY_VERSION" >/dev/null
else
  "$RUBY_BIN" -S gem install honker >/dev/null
fi
"$RUBY_BIN" <<'RB'
require "tmpdir"
require "honker"

Dir.mktmpdir("honker-ruby-registry-") do |dir|
  db = Honker::Database.new(File.join(dir, "app.db"))
  q = db.queue("emails")
  id = q.enqueue({ to: "alice@example.com" })
  job = q.claim_one("worker-1")
  raise "job id mismatch" unless job.id == id
  raise "payload mismatch" unless job.payload["to"] == "alice@example.com"
  raise "ack failed" unless job.ack

  db.db.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
  orders = db.queue("orders")
  db.transaction do |tx|
    tx.execute("INSERT INTO orders VALUES (?, ?)", [1, 100])
    orders.enqueue_tx(tx, { order_id: 1 })
    tx.rollback!
  end
  raise "rollback leaked" unless db.db.get_first_value("SELECT COUNT(*) FROM orders") == 0

  db.transaction do |tx|
    tx.execute("INSERT INTO orders VALUES (?, ?)", [2, 200])
    orders.enqueue_tx(tx, { order_id: 2 })
  end
  raise "commit failed" unless orders.claim_one("w").payload["order_id"] == 2

  stream = db.stream("events")
  off = stream.publish({ id: 1 })
  event = stream.read_since(0, 10).first
  raise "stream mismatch" unless event.offset == off && event.payload["id"] == 1
  raise "offset save failed" unless stream.save_offset("billing", off)
  raise "offset mismatch" unless stream.get_offset("billing") == off

  db.notify("orders", { id: 1 })
  count = db.db.get_first_value("SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'")
  raise "notify failed" unless count == 1
  db.close
end
puts "ruby registry gauntlet ok"
RB

echo "== dotnet registry gauntlet =="
mkdir -p "$TMP/dotnet"
(
  cd "$TMP/dotnet"
  dotnet new console >/dev/null
  if [[ -n "$HONKER_DOTNET_VERSION" ]]; then
    dotnet add package Honker --version "$HONKER_DOTNET_VERSION" >/dev/null
  else
    dotnet add package Honker >/dev/null
  fi
  cat > Program.cs <<'CS'
using Honker;

var dir = Path.Combine(Path.GetTempPath(), $"honker-dotnet-registry-{Guid.NewGuid():N}");
Directory.CreateDirectory(dir);
try
{
    using var db = Database.Open(Path.Combine(dir, "app.db"));
    var q = db.Queue("emails");
    var id = q.Enqueue(new { to = "alice@example.com" });
    var job = q.ClaimOne("worker-1") ?? throw new Exception("claim failed");
    if (job.Id != id) throw new Exception("id mismatch");
    if (job.Payload.GetProperty("to").GetString() != "alice@example.com") throw new Exception("payload mismatch");
    if (!job.Ack()) throw new Exception("ack failed");

    db.Execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)");
    var orders = db.Queue("orders");
    using (var tx = db.BeginTransaction())
    {
        tx.Execute("INSERT INTO orders VALUES (@p0, @p1)", 1, 100);
        orders.Enqueue(new { orderId = 1 }, transaction: tx);
        tx.Rollback();
    }
    if (Convert.ToInt64(db.Query("SELECT COUNT(*) AS c FROM orders")[0]["c"]) != 0) throw new Exception("rollback leaked");

    using (var tx = db.BeginTransaction())
    {
        tx.Execute("INSERT INTO orders VALUES (@p0, @p1)", 2, 200);
        orders.Enqueue(new { orderId = 2 }, transaction: tx);
        tx.Commit();
    }
    if (orders.ClaimOne("w")!.Payload.GetProperty("orderId").GetInt32() != 2) throw new Exception("commit job missing");

    var stream = db.Stream("events");
    var off = stream.Publish(new { id = 1 });
    var ev = stream.ReadSince(0, 10)[0];
    if (ev.Offset != off || ev.Payload.GetProperty("id").GetInt32() != 1) throw new Exception("stream mismatch");
    stream.SaveOffset("billing", off);
    if (stream.GetOffset("billing") != off) throw new Exception("offset mismatch");

    db.Notify("orders", new { id = 1 });
    if (Convert.ToInt64(db.Query("SELECT COUNT(*) AS c FROM _honker_notifications WHERE channel='orders'")[0]["c"]) != 1) throw new Exception("notify failed");
    Console.WriteLine("dotnet registry gauntlet ok");
}
finally
{
    Directory.Delete(dir, recursive: true);
}
CS
  dotnet run --no-restore
)

echo "== elixir registry gauntlet =="
mkdir -p "$TMP/elixir"
(
  cd "$TMP/elixir"
  cat > mix.exs <<MIX
defmodule HonkerGauntlet.MixProject do
  use Mix.Project
  def project do
    [
      app: :honker_gauntlet,
      version: "0.1.0",
      elixir: "~> 1.17",
      deps: deps()
    ]
  end
  def application, do: [extra_applications: [:logger]]
  defp deps do
    [{:honker, "${HONKER_ELIXIR_VERSION:+== }${HONKER_ELIXIR_VERSION:-~> 0.1}"}]
  end
end
MIX
  mix local.hex --force >/dev/null
  mix deps.get >/dev/null
  cat > gauntlet.exs <<'EX'
alias Honker.{Job, Queue, Stream, Transaction}

dir = Path.join(System.tmp_dir!(), "honker-ex-registry-#{System.unique_integer([:positive])}")
File.mkdir_p!(dir)
db_path = Path.join(dir, "app.db")
{:ok, db} = Honker.open(db_path, extension_path: System.fetch_env!("HONKER_EXTENSION_PATH"))

{:ok, id} = Queue.enqueue(db, "emails", %{"to" => "alice@example.com"})
{:ok, job} = Queue.claim_one(db, "emails", "worker-1")
true = job.id == id
%{"to" => "alice@example.com"} = job.payload
{:ok, true} = Job.ack(db, job)

:ok = Exqlite.Sqlite3.execute(db.conn, "CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
{:ok, tx} = Transaction.begin(db)
:ok = Transaction.execute(tx, "INSERT INTO orders VALUES (?1, ?2)", [1, 100])
{:ok, _} = Queue.enqueue_tx(tx, "orders", %{"order_id" => 1}, [], [])
:ok = Transaction.rollback(tx)
{:ok, [0]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM orders", [])

{:ok, tx2} = Transaction.begin(db)
:ok = Transaction.execute(tx2, "INSERT INTO orders VALUES (?1, ?2)", [2, 200])
{:ok, _} = Queue.enqueue_tx(tx2, "orders", %{"order_id" => 2}, [], [])
:ok = Transaction.commit(tx2)
{:ok, job2} = Queue.claim_one(db, "orders", "w")
%{"order_id" => 2} = job2.payload

{:ok, off} = Stream.publish(db, "events", %{"id" => 1})
{:ok, [event]} = Stream.read_since(db, "events", 0, 10)
true = event.offset == off
%{"id" => 1} = event.payload
{:ok, true} = Stream.save_offset(db, "billing", "events", off)
{:ok, ^off} = Stream.get_offset(db, "billing", "events")

{:ok, _} = Honker.notify(db, "orders", %{"id" => 1})
{:ok, [1]} = Honker.query_first(db.conn, "SELECT COUNT(*) FROM _honker_notifications WHERE channel='orders'", [])
Honker.close(db)
File.rm_rf!(dir)
IO.puts("elixir registry gauntlet ok")
EX
  HONKER_EXTENSION_PATH="$EXT_PATH" mix run gauntlet.exs
)

echo "== rust crates.io gauntlet =="
mkdir -p "$TMP/rust/src"
(
  cd "$TMP/rust"
  cat > Cargo.toml <<TOML
[package]
name = "honker_rust_gauntlet"
version = "0.1.0"
edition = "2024"

[dependencies]
honker = "${HONKER_RUST_VERSION:+=}${HONKER_RUST_VERSION:-*}"
serde_json = "1"
tempfile = "3"
TOML
  cat > src/main.rs <<'RS'
use serde_json::json;
use std::time::Duration;

fn main() -> honker::Result<()> {
    let dir = tempfile::tempdir().unwrap();
    let db = honker::Database::open(dir.path().join("app.db"))?;
    let q = db.queue("emails", honker::QueueOpts::default());
    let id = q.enqueue(&json!({"to":"alice@example.com"}), honker::EnqueueOpts::default())?;
    let job = q.claim_one("worker-1")?.expect("claim failed");
    assert_eq!(job.id, id);
    assert_eq!(job.payload_as::<serde_json::Value>()?["to"], "alice@example.com");
    assert!(job.ack()?);

    let orders = db.queue("orders", honker::QueueOpts::default());
    {
        let tx = db.transaction()?;
        orders.enqueue_tx(&tx, &json!({"order_id":1}), honker::EnqueueOpts::default())?;
        tx.rollback()?;
    }
    assert!(orders.claim_one("w")?.is_none());

    {
        let tx = db.transaction()?;
        orders.enqueue_tx(&tx, &json!({"order_id":2}), honker::EnqueueOpts::default())?;
        tx.commit()?;
    }
    assert_eq!(orders.claim_one("w")?.unwrap().payload_as::<serde_json::Value>()?["order_id"], 2);

    let stream = db.stream("events");
    let off = stream.publish(&json!({"id":1}))?;
    let events = stream.read_since(0, 10)?;
    assert_eq!(events[0].offset, off);
    assert_eq!(events[0].payload_as::<serde_json::Value>()?["id"], 1);
    assert!(stream.save_offset("billing", off)?);
    assert_eq!(stream.get_offset("billing")?, off);

    let mut sub = db.listen("orders")?;
    db.notify("orders", &json!({"id":1}))?;
    let note = sub.recv_timeout(Duration::from_secs(2))?.expect("notification missing");
    assert_eq!(note.payload_as::<serde_json::Value>()?["id"], 1);
    println!("rust crates.io gauntlet ok");
    Ok(())
}
RS
  cargo run --quiet
)

echo "== go module gauntlet =="
mkdir -p "$TMP/go"
(
  cd "$TMP/go"
  go mod init honker-go-gauntlet >/dev/null
  go get "github.com/russellromney/honker-go@$HONKER_GO_REF" >/dev/null
  cat > main.go <<'GO'
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	honker "github.com/russellromney/honker-go"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	ext := os.Getenv("HONKER_EXTENSION_PATH")
	if ext == "" {
		panic("HONKER_EXTENSION_PATH is required")
	}
	dir, err := os.MkdirTemp("", "honker-go-gauntlet-")
	must(err)
	defer os.RemoveAll(dir)
	db, err := honker.Open(filepath.Join(dir, "app.db"), ext)
	must(err)
	defer db.Close()

	q := db.Queue("emails", honker.QueueOptions{})
	id, err := q.Enqueue(map[string]any{"to": "alice@example.com"}, honker.EnqueueOptions{})
	must(err)
	job, err := q.ClaimOne("worker-1")
	must(err)
	if job == nil || job.ID != id {
		panic("claim mismatch")
	}
	var payload map[string]any
	must(json.Unmarshal(job.Payload, &payload))
	if payload["to"] != "alice@example.com" {
		panic("payload mismatch")
	}
	ok, err := job.Ack()
	must(err)
	if !ok {
		panic("ack failed")
	}

	_, err = db.Raw().Exec("CREATE TABLE orders (id INTEGER PRIMARY KEY, total INTEGER)")
	must(err)
	orders := db.Queue("orders", honker.QueueOptions{})
	tx, err := db.Begin()
	must(err)
	_, err = tx.Exec("INSERT INTO orders VALUES (?, ?)", 1, 100)
	must(err)
	_, err = orders.EnqueueTx(tx, map[string]any{"order_id": 1}, honker.EnqueueOptions{})
	must(err)
	must(tx.Rollback())
	var count int
	must(db.Raw().QueryRow("SELECT COUNT(*) FROM orders").Scan(&count))
	if count != 0 {
		panic("rollback leaked")
	}
	tx, err = db.Begin()
	must(err)
	_, err = tx.Exec("INSERT INTO orders VALUES (?, ?)", 2, 200)
	must(err)
	_, err = orders.EnqueueTx(tx, map[string]any{"order_id": 2}, honker.EnqueueOptions{})
	must(err)
	must(tx.Commit())
	job, err = orders.ClaimOne("w")
	must(err)
	if job == nil {
		panic("committed job missing")
	}
	must(json.Unmarshal(job.Payload, &payload))
	if payload["order_id"].(float64) != 2 {
		panic("committed payload mismatch")
	}

	s := db.Stream("events")
	off, err := s.Publish(map[string]any{"id": 1})
	must(err)
	events, err := s.ReadSince(0, 10)
	must(err)
	if len(events) != 1 || events[0].Offset != off {
		panic("stream mismatch")
	}
	ok, err = s.SaveOffset("billing", off)
	must(err)
	if !ok {
		panic("offset save failed")
	}
	saved, err := s.GetOffset("billing")
	must(err)
	if saved != off {
		panic("offset mismatch")
	}

	_, err = db.Notify("orders", map[string]any{"id": 1})
	must(err)
	fmt.Println("go module gauntlet ok")
}
GO
  HONKER_EXTENSION_PATH="$EXT_PATH" go run .
)

echo "== c++ source archive gauntlet =="
mkdir -p "$TMP/cpp"
(
  cd "$TMP/cpp"
  curl -fsSL "https://github.com/russellromney/honker/archive/${HONKER_CPP_REF}.tar.gz" -o honker.tar.gz
  tar -xzf honker.tar.gz --strip-components=1
  cd packages/honker-cpp
  zig build test -Dhonker-ext="$EXT_PATH"
)

echo "registry/source install gauntlet ok"
