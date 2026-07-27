import gc
import os
import sys
import tempfile

import pytest

# Put packages/ on sys.path so the `honker` package is importable in
# tests without needing a `pip install -e`.
_REPO_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_PACKAGES_ROOT = os.path.join(_REPO_ROOT, "packages")
_HONKER_PYTHON_ROOT = os.path.join(_PACKAGES_ROOT, "honker", "python")
for path in (_HONKER_PYTHON_ROOT, _PACKAGES_ROOT):
    if os.path.isdir(path) and path not in sys.path:
        sys.path.insert(0, path)


@pytest.fixture
def db_path():
    # ignore_cleanup_errors: the gc.collect() below cannot free `db`
    # when a test *fails* — pytest keeps the traceback alive for its
    # report, and the traceback pins the frame that holds the reference.
    # On Windows that turns any failing test into a second, confusing
    # teardown ERROR (WinError 32) on top of the real one. A leaked temp
    # dir on a CI runner costs nothing; a phantom error costs triage time.
    with tempfile.TemporaryDirectory(ignore_cleanup_errors=True) as d:
        yield os.path.join(d, "t.db")
        # Pytest captures test-function locals for failure reporting,
        # so the test's `db = honker.open(path)` reference can outlive
        # the test body and delay Database's Drop until after the
        # `with` exits. On Linux/macOS unlink-while-open hides this;
        # on Windows tempfile cleanup hits WinError 32.
        # Force a collection cycle here so Drop runs and releases the
        # SQLite handles before TemporaryDirectory.__exit__ unlinks.
        gc.collect()
