"""User-facing stream-checkpoint interoperability for Node and Python."""

import asyncio
import json
import shutil
import subprocess
from pathlib import Path

import pytest

import honker


REPO_ROOT = Path(__file__).resolve().parents[1]
NODE_DIR = REPO_ROOT / "packages" / "honker-node"


def _node_command():
    node = shutil.which("node")
    return [node] if node else None


def _node_ready(cmd):
    if cmd is None:
        return False
    probe = subprocess.run(
        [*cmd, "-e", 'require(".")'],
        cwd=NODE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    return probe.returncode == 0


NODE_CMD = _node_command()


def _require_node_interop():
    if _node_ready(NODE_CMD):
        return
    pytest.skip(
        "Node binding unavailable; install node and run `npm ci` plus "
        "`npm run build` in packages/honker-node"
    )


def _run_node(script, db_path, *args):
    proc = subprocess.run(
        [*NODE_CMD, "-e", script, str(db_path), *map(str, args)],
        cwd=NODE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


def _publish_python_events(stream, db, count=3):
    for i in range(count):
        stream.publish({"i": i})
    return db.query(
        "SELECT offset FROM _honker_stream WHERE topic=? ORDER BY offset", [stream.name]
    )


def test_python_checkpoint_drives_node_read_from_consumer(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "python-to-node-checkpoint.db"
    py_db = honker.open(str(db_path))
    try:
        stream = py_db.stream("orders")
        offsets = _publish_python_events(stream, py_db)
        stream.save_offset("worker-c", offsets[0]["offset"])

        observed = _run_node(
            r"""
            const honker = require(".");
            const db = honker.open(process.argv[1]);
            try {
              const stream = db.stream("orders");
              const events = stream.readFromConsumer("worker-c", 10);
              stream.saveOffset("worker-c", events.at(-1).offset);
              console.log(JSON.stringify({
                payloads: events.map((event) => event.payload),
                savedOffset: stream.getOffset("worker-c"),
              }));
            } finally {
              db.close();
            }
            """,
            db_path,
        )

        assert observed == {
            "payloads": [{"i": 1}, {"i": 2}],
            "savedOffset": offsets[2]["offset"],
        }
        assert stream.get_offset("worker-c") == offsets[2]["offset"]
    finally:
        py_db.close()


def test_node_checkpoint_drives_python_named_subscription(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "node-to-python-checkpoint.db"
    observed = _run_node(
        r"""
        const honker = require(".");
        const db = honker.open(process.argv[1]);
        try {
          const stream = db.stream("orders");
          const offsets = [0, 1, 2].map((i) => stream.publish({ i }));
          stream.saveOffset("worker-py", offsets[0]);
          console.log(JSON.stringify({ offsets }));
        } finally {
          db.close();
        }
        """,
        db_path,
    )

    py_db = honker.open(str(db_path))
    try:
        stream = py_db.stream("orders")
        assert stream.get_offset("worker-py") == observed["offsets"][0]

        async def consume_remaining():
            iterator = stream.subscribe(consumer="worker-py")
            return [
                (await iterator.__anext__()).payload,
                (await iterator.__anext__()).payload,
            ]

        assert asyncio.run(consume_remaining()) == [{"i": 1}, {"i": 2}]
    finally:
        py_db.close()


def test_node_subscription_resumes_from_python_and_persists_progress(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "node-subscription-checkpoint.db"
    py_db = honker.open(str(db_path))
    try:
        stream = py_db.stream("orders")
        offsets = _publish_python_events(stream, py_db)
        stream.save_offset("node-subscriber", offsets[0]["offset"])

        observed = _run_node(
            r"""
            const honker = require(".");
            (async () => {
              const db = honker.open(process.argv[1]);
              try {
                const stream = db.stream("orders");
                const subscription = stream.subscribe("node-subscriber");
                const next = await subscription.next();
                subscription.close();
                console.log(JSON.stringify({
                  payload: next.value.payload,
                  offset: next.value.offset,
                }));
              } finally {
                db.close();
              }
            })().catch((err) => {
              console.error(err);
              process.exitCode = 1;
            });
            """,
            db_path,
        )

        assert observed == {"payload": {"i": 1}, "offset": offsets[1]["offset"]}
        assert stream.get_offset("node-subscriber") == offsets[1]["offset"]
    finally:
        py_db.close()


def test_node_automatically_migrates_verified_0_4_6_checkpoint(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "legacy-node-checkpoint.db"
    py_db = honker.open(str(db_path))
    try:
        stream = py_db.stream("orders")
        offsets = _publish_python_events(stream, py_db)
        with py_db.transaction() as tx:
            # Node 0.4.6 wrote (stream, consumer) where the ABI expected
            # (consumer, stream).
            tx.execute(
                "INSERT INTO _honker_stream_consumers (name, topic, offset) "
                "VALUES (?, ?, ?)",
                ["orders", "worker-c", offsets[0]["offset"]],
            )

        observed = _run_node(
            r"""
            const honker = require(".");
            const db = honker.open(process.argv[1]);
            try {
              const stream = db.stream("orders");
              const events = stream.readFromConsumer("worker-c", 10);
              console.log(JSON.stringify({
                offset: stream.getOffset("worker-c"),
                payloads: events.map((event) => event.payload),
              }));
            } finally {
              db.close();
            }
            """,
            db_path,
        )

        assert observed == {
            "offset": offsets[0]["offset"],
            "payloads": [{"i": 1}, {"i": 2}],
        }
        assert stream.get_offset("worker-c") == offsets[0]["offset"]
        rows = py_db.query(
            "SELECT name, topic FROM _honker_stream_consumers ORDER BY name, topic"
        )
        assert rows == [
            {"name": "orders", "topic": "worker-c"},
            {"name": "worker-c", "topic": "orders"},
        ]
    finally:
        py_db.close()


def test_node_transactional_checkpoint_is_visible_to_python(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "node-transactional-checkpoint.db"
    py_db = honker.open(str(db_path))
    try:
        stream = py_db.stream("orders")
        offsets = _publish_python_events(stream, py_db)

        observed = _run_node(
            r"""
            const honker = require(".");
            const db = honker.open(process.argv[1]);
            try {
              const stream = db.stream("orders");
              const committed = db.transaction();
              stream.saveOffsetTx(committed, "worker-c", Number(process.argv[2]));
              committed.commit();

              const rolledBack = db.transaction();
              stream.saveOffsetTx(rolledBack, "worker-c", Number(process.argv[3]));
              rolledBack.rollback();
              console.log(JSON.stringify({ offset: stream.getOffset("worker-c") }));
            } finally {
              db.close();
            }
            """,
            db_path,
            offsets[1]["offset"],
            offsets[2]["offset"],
        )

        assert observed["offset"] == offsets[1]["offset"]
        assert stream.get_offset("worker-c") == offsets[1]["offset"]
    finally:
        py_db.close()
