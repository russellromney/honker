//! A lock conflict inside a `honker_*` SQL function reaches the caller
//! with its SQLite code, not SQLITE_ERROR. Everything else keeps
//! SQLITE_ERROR and its message.
//!
//! These go through `SELECT honker_*(...)`, so they cover the whole
//! path: the function's error, rusqlite's `report_error`, the VDBE and
//! `sqlite3_step`. tests/test_extension_error_codes.py checks the same
//! through the loadable extension from another process.
use crate::{attach_honker_functions, attach_notify, bootstrap_honker_schema, honker_ops};
use rusqlite::{Connection, Error, ffi, types::Value};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

struct TempDb {
    dir: std::path::PathBuf,
    path: std::path::PathBuf,
}

impl Drop for TempDb {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn temp_db() -> TempDb {
    let dir = std::env::temp_dir().join(format!(
        "honker-busy-codes-{}-{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("t.db");
    TempDb { dir, path }
}

fn connect(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
        .unwrap();
    attach_notify(&conn).unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    conn
}

fn codes(err: &Error) -> (i32, &str) {
    match err {
        Error::SqliteFailure(e, msg) => (e.extended_code, msg.as_deref().unwrap_or("")),
        other => panic!("expected a SQLite failure, got {other:?}"),
    }
}

fn call(conn: &Connection, sql: &str) -> rusqlite::Result<Value> {
    conn.query_row(sql, [], |r| r.get(0))
}

#[test]
fn exclusive_lock_from_another_connection_is_sqlite_busy() {
    let db = temp_db();
    let setup = connect(&db.path);
    honker_ops::enqueue(&setup, "q", "{}", None, None, 0, 3, None).unwrap();
    honker_ops::enqueue(&setup, "q", "{}", None, None, 0, 3, None).unwrap();
    let claimed = honker_ops::claim_batch(&setup, "q", "w", 1, 300).unwrap();
    let id = serde_json::from_str::<serde_json::Value>(&claimed).unwrap()[0]["id"]
        .as_i64()
        .unwrap();
    honker_ops::scheduler_register(&setup, "t", "q", "@every 1s", "{}", 0, None, 3).unwrap();

    let caller = connect(&db.path);
    setup.execute_batch("BEGIN EXCLUSIVE").unwrap();
    for sql in [
        "SELECT honker_claim_batch('q', 'w2', 1, 30)".to_string(),
        format!("SELECT honker_ack({id}, 'w')"),
        format!("SELECT honker_ack({id}, 'w', 1)"),
        "SELECT honker_scheduler_tick(unixepoch() + 3600)".to_string(),
        "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)".to_string(),
        "SELECT notify('ch', '{}')".to_string(),
    ] {
        let err = call(&caller, &sql).expect_err(&sql);
        assert_eq!(
            codes(&err),
            (ffi::SQLITE_BUSY, "database is locked"),
            "{sql}: {err:?}"
        );
        assert!(caller.is_autocommit(), "{sql} left a transaction open");
    }
    setup.execute_batch("ROLLBACK").unwrap();
    // Transient: the same call works once the lock is gone.
    call(&caller, "SELECT honker_claim_batch('q', 'w2', 1, 30)").unwrap();
}

/// The extended code survives too. A caller's read snapshot that another
/// connection's commit replaced cannot be upgraded to a write:
/// SQLITE_BUSY_SNAPSHOT (517).
#[test]
fn stale_snapshot_keeps_extended_busy_snapshot_code() {
    let db = temp_db();
    let caller = connect(&db.path);
    let other = connect(&db.path);
    caller
        .execute_batch("BEGIN; SELECT count(*) FROM _honker_live;")
        .unwrap();
    honker_ops::enqueue(&other, "q", "{}", None, None, 0, 3, None).unwrap();
    let err = call(
        &caller,
        "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 3, NULL)",
    )
    .unwrap_err();
    assert_eq!(
        codes(&err),
        (ffi::SQLITE_BUSY_SNAPSHOT, "database is locked")
    );
    caller.execute_batch("ROLLBACK").unwrap();
}

#[test]
fn non_transient_errors_keep_sqlite_error_and_message() {
    let db = temp_db();
    let caller = connect(&db.path);
    let err = call(
        &caller,
        "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, 0, NULL)",
    )
    .unwrap_err();
    let (code, msg) = codes(&err);
    assert_eq!(code, ffi::SQLITE_ERROR, "{err:?}");
    assert!(msg.contains("max_attempts must be at least 1"), "{msg}");

    // #167: SQLite raises this as SQLITE_BUSY, but no retry fixes it.
    caller.execute_batch("CREATE TABLE app(x)").unwrap();
    let err = caller
        .execute_batch("INSERT INTO app SELECT honker_claim_batch('q', 'w', 1, 30)")
        .unwrap_err();
    let (code, msg) = codes(&err);
    assert_eq!(code, ffi::SQLITE_ERROR, "{err:?}");
    assert!(
        msg.contains("cannot open savepoint - SQL statements in progress")
            && msg.contains("honker: honker_claim_batch requires a separate SELECT"),
        "{msg}"
    );
}
