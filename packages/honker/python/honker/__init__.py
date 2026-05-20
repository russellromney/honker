import os
import sys
from pathlib import Path

from honker._honker import (
    Database,
    Event,
    Job,
    Listener,
    LockHeld,
    Notification,
    Outbox,
    Queue,
    Retryable,
    Stream,
    open,
)
from honker._scheduler import (
    CronSchedule,
    Scheduler,
    crontab,
    every_s,
)
from honker._tasks import (
    TaskResult,
    UnknownTaskError,
    registry,
    run_workers,
)

EXTENSION_ENTRYPOINT = "sqlite3_honkerext_init"


def _extension_filenames() -> tuple[str, ...]:
    if sys.platform == "win32":
        return ("honker_ext.dll",)
    if sys.platform == "darwin":
        return ("libhonker_ext.dylib",)
    return ("libhonker_ext.so",)


def _extension_candidates() -> list[Path]:
    names = _extension_filenames()
    candidates: list[Path] = []

    package_dir = Path(__file__).resolve().parent
    candidates.extend(package_dir / "_lib" / name for name in names)

    for parent in package_dir.parents:
        candidates.extend(parent / "target" / "release" / name for name in names)

    return candidates


def extension_info() -> tuple[str, str]:
    """Return the bundled SQLite extension path and entrypoint.

    This is for users who want to load Honker into their own sqlite3,
    SQLAlchemy, or other SQLite connection instead of using honker.open().
    Honker does not wrap those clients; it just exposes the loadable
    extension artifact when one is available.
    """
    env_path = os.environ.get("HONKER_EXTENSION_PATH")
    if env_path:
        candidate = Path(env_path)
        if candidate.is_file():
            return str(candidate), EXTENSION_ENTRYPOINT
        raise FileNotFoundError(f"HONKER_EXTENSION_PATH does not exist: {candidate}")

    for candidate in _extension_candidates():
        if candidate.is_file():
            return str(candidate), EXTENSION_ENTRYPOINT
    searched = ", ".join(str(p) for p in _extension_candidates())
    raise FileNotFoundError(f"Honker SQLite extension not found; searched: {searched}")


def load_extension(conn) -> None:
    """Load Honker's SQLite extension into an existing DB-API connection."""
    path, entrypoint = extension_info()
    enable = getattr(conn, "enable_load_extension", None)
    if enable is not None:
        enable(True)
    try:
        load = getattr(conn, "load_extension", None)
        if load is None:
            conn.execute("SELECT load_extension(?, ?)", (path, entrypoint))
        else:
            try:
                load(path, entrypoint=entrypoint)
            except TypeError:
                conn.execute("SELECT load_extension(?, ?)", (path, entrypoint))
    finally:
        if enable is not None:
            enable(False)


__all__ = [
    "CronSchedule",
    "Database",
    "Event",
    "Job",
    "Listener",
    "LockHeld",
    "Notification",
    "Outbox",
    "Queue",
    "Retryable",
    "Scheduler",
    "Stream",
    "TaskResult",
    "UnknownTaskError",
    "EXTENSION_ENTRYPOINT",
    "crontab",
    "every_s",
    "extension_info",
    "load_extension",
    "open",
    "registry",
    "run_workers",
]
