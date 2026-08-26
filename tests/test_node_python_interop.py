"""Cross-runtime stream-checkpoint interoperability for Node and Python."""

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


def _run_node(script, db_path):
    proc = subprocess.run(
        [*NODE_CMD, "-e", script, str(db_path)],
        cwd=NODE_DIR,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        check=True,
    )
    return json.loads(proc.stdout)


@pytest.mark.xfail(
    strict=True,
    raises=AssertionError,
    reason="Node 0.4.6 transposes the stream checkpoint consumer and topic",
)
def test_node_and_python_share_stream_checkpoints(tmp_path):
    _require_node_interop()
    db_path = tmp_path / "node-python-checkpoint.db"
    stream_name = "orders"
    consumer = "worker-c"
    python_offset = 17
    node_offset = 29

    py_db = honker.open(str(db_path))
    try:
        py_stream = py_db.stream(stream_name)
        py_stream.save_offset(consumer, python_offset)

        observed = _run_node(
            r'''
            const honker = require(".");

            const db = honker.open(process.argv[1]);
            try {
              const stream = db.stream("orders");
              const pythonOffsetSeenByNode = stream.getOffset("worker-c");
              stream.saveOffset("worker-c", 29);
              console.log(JSON.stringify({ pythonOffsetSeenByNode }));
            } finally {
              db.close();
            }
            ''',
            db_path,
        )
        node_offset_seen_by_python = py_stream.get_offset(consumer)
    finally:
        py_db.close()

    assert (
        observed["pythonOffsetSeenByNode"] == python_offset
        and node_offset_seen_by_python == node_offset
    ), (
        "Node stream checkpoint key transposition: Python writes "
        f"(consumer={consumer!r}, topic={stream_name!r}) but Node reads/writes "
        f"(consumer={stream_name!r}, topic={consumer!r}); "
        f"Node saw {observed['pythonOffsetSeenByNode']} instead of {python_offset}, "
        f"and Python saw {node_offset_seen_by_python} instead of {node_offset}"
    )
