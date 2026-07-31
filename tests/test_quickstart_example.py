import os
import sqlite3
import subprocess
import sys
from pathlib import Path


def test_quickstart_runs_end_to_end_under_optimized_python(tmp_path):
    root = Path(__file__).resolve().parents[1]
    script = root / "packages" / "honker" / "examples" / "quickstart.py"
    db_path = tmp_path / "quickstart.db"
    env = os.environ.copy()
    env["HONKER_QUICKSTART_DB"] = str(db_path)

    result = subprocess.run(
        [sys.executable, "-O", str(script)],
        cwd=root,
        check=True,
        capture_output=True,
        env=env,
        text=True,
        timeout=15,
    )

    assert "sending email to alice@example.com: Receipt for 19.99" in result.stdout
    assert result.stdout.rstrip().endswith("done")
    with sqlite3.connect(db_path) as con:
        assert con.execute("SELECT COUNT(*) FROM orders").fetchone() == (1,)
        assert con.execute(
            "SELECT COUNT(*) FROM _honker_live"
        ).fetchone() == (0,)
