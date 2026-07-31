import subprocess
import sys
from pathlib import Path


def test_quickstart_runs_end_to_end():
    root = Path(__file__).resolve().parents[1]
    script = root / "packages" / "honker" / "examples" / "quickstart.py"

    result = subprocess.run(
        [sys.executable, str(script)],
        cwd=root,
        check=True,
        capture_output=True,
        text=True,
        timeout=15,
    )

    assert "sending email to alice@example.com: Receipt for 19.99" in result.stdout
    assert result.stdout.rstrip().endswith("done")
