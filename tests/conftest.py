import gc
import os
import shutil
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


_FAILED_NODES = set()


def pytest_runtest_logreport(report):
    # Record which tests failed so fixtures can tell, during teardown,
    # whether the test itself passed. Used by `db_path` below.
    #
    # `logreport` rather than a `makereport` hookwrapper on purpose: it
    # is a plain hook (no yield), so it is unaffected by pytest's
    # migration of `hookwrapper=True` to `wrapper=True`, and CI installs
    # pytest unpinned. The setup and call reports are both emitted
    # before the teardown phase runs, so the flag is set by the time the
    # fixture finalizer below looks at it.
    if report.failed:
        _FAILED_NODES.add(report.nodeid)


def _test_failed(node):
    return node.nodeid in _FAILED_NODES


@pytest.fixture
def db_path(request):
    # Cleanup is strict on purpose. A WinError 32 here means SQLite
    # handles outlived the Database, which is the exact bug honker-core's
    # explicit `close()` (PR #18) was written to fix — masking it would
    # retire the only regression signal we have for that fix, and
    # symptom-treating this in the harness was already rejected once
    # (PR #14, closed as superseded by #18).
    #
    # The one case we do tolerate: when a test *fails*, pytest keeps the
    # traceback alive for its report, and that traceback pins the frame
    # holding `db`, so the gc.collect() below cannot free it. On Windows
    # that turns any failing test into a second, confusing teardown ERROR
    # on top of the real one. Ignore cleanup errors only in that case — a
    # leaked temp dir on a CI runner costs nothing, a phantom error costs
    # triage time, and a passing test still has to unlink cleanly.
    d = tempfile.mkdtemp()
    try:
        yield os.path.join(d, "t.db")
        # Pytest captures test-function locals for failure reporting,
        # so the test's `db = honker.open(path)` reference can outlive
        # the test body and delay Database's Drop until after the
        # fixture returns. On Linux/macOS unlink-while-open hides this;
        # on Windows cleanup hits WinError 32.
        # Force a collection cycle here so Drop runs and releases the
        # SQLite handles before we unlink.
        gc.collect()
    finally:
        shutil.rmtree(d, ignore_errors=_test_failed(request.node))
