import sqlite3

import pytest

import honker


def test_extension_info_honors_env_override(tmp_path, monkeypatch):
    fake_ext = tmp_path / "custom_ext.so"
    fake_ext.write_bytes(b"not really a dylib")
    monkeypatch.setenv("HONKER_EXTENSION_PATH", str(fake_ext))

    assert honker.extension_info() == (str(fake_ext), "sqlite3_honkerext_init")


def test_extension_info_rejects_missing_env_override(tmp_path, monkeypatch):
    missing = tmp_path / "missing_ext.so"
    monkeypatch.setenv("HONKER_EXTENSION_PATH", str(missing))

    with pytest.raises(FileNotFoundError, match="HONKER_EXTENSION_PATH"):
        honker.extension_info()


def test_load_extension_falls_back_to_sql_entrypoint(tmp_path, monkeypatch):
    fake_ext = tmp_path / "custom_ext.so"
    fake_ext.write_bytes(b"not really a dylib")
    monkeypatch.setenv("HONKER_EXTENSION_PATH", str(fake_ext))

    calls = []

    class Conn:
        def enable_load_extension(self, enabled):
            calls.append(("enable", enabled))

        def load_extension(self, path, *, entrypoint=None):
            calls.append(("method", path, entrypoint))
            raise TypeError("old sqlite3 signature")

        def execute(self, sql, params):
            calls.append(("execute", sql, params))

    honker.load_extension(Conn())

    assert calls == [
        ("enable", True),
        ("method", str(fake_ext), "sqlite3_honkerext_init"),
        (
            "execute",
            "SELECT load_extension(?, ?)",
            (str(fake_ext), "sqlite3_honkerext_init"),
        ),
        ("enable", False),
    ]


def test_transaction_execute_and_query_accept_tuple_params(tmp_path):
    db = honker.open(str(tmp_path / "tuple-params.db"))

    with db.transaction() as tx:
        tx.execute("CREATE TABLE emails (id INTEGER PRIMARY KEY, object_id TEXT)")
        tx.execute("INSERT INTO emails (object_id) VALUES (?)", ("msg-1",))
        rows = tx.query("SELECT id FROM emails WHERE object_id = ?", ("msg-1",))

    assert rows == [{"id": 1}]


def test_query_rejects_dict_params(tmp_path):
    db = honker.open(str(tmp_path / "dict-params.db"))

    with pytest.raises(TypeError, match="positional sequence"):
        db.query("SELECT ?", {"value": 1})


def test_extension_info_loads_sqlite_extension_when_available(tmp_path):
    if not hasattr(sqlite3.connect(":memory:"), "enable_load_extension"):
        pytest.skip("stdlib sqlite3 was built without load-extension support")

    try:
        path, entrypoint = honker.extension_info()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))

    conn = sqlite3.connect(str(tmp_path / "extension-info.db"))
    conn.enable_load_extension(True)
    try:
        conn.load_extension(path, entrypoint=entrypoint)
    except TypeError:
        conn.execute("SELECT load_extension(?, ?)", (path, entrypoint))
    conn.execute("SELECT honker_bootstrap()")


def test_load_extension_helper_loads_sqlite_extension_when_available(tmp_path):
    if not hasattr(sqlite3.connect(":memory:"), "enable_load_extension"):
        pytest.skip("stdlib sqlite3 was built without load-extension support")

    try:
        honker.extension_info()
    except FileNotFoundError as exc:
        pytest.skip(str(exc))

    conn = sqlite3.connect(str(tmp_path / "load-helper.db"))
    honker.load_extension(conn)
    conn.execute("SELECT honker_bootstrap()")


def test_queue_memoized_same_options(db_path):
    db = honker.open(db_path)
    q1 = db.queue("emails", visibility_timeout_s=300, max_attempts=3)
    q2 = db.queue("emails", visibility_timeout_s=300, max_attempts=3)
    assert q1 is q2


def test_queue_conflicting_options_raises(db_path):
    db = honker.open(db_path)
    db.queue("emails", visibility_timeout_s=300, max_attempts=3)
    with pytest.raises(ValueError, match="already opened"):
        db.queue("emails", visibility_timeout_s=60, max_attempts=3)
    with pytest.raises(ValueError, match="already opened"):
        db.queue("emails", visibility_timeout_s=300, max_attempts=10)
