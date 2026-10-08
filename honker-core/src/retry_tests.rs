//! Retry must not act on a claim that another connection replaced, and must
//! not fail because another connection committed while it ran.
use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

fn connect(path: &std::path::Path, clock: &Arc<AtomicI64>) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
        .unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    let clock = clock.clone();
    // Control only time, not ownership or results. Avoid a subsecond deadline
    // test that fails if a loaded CI runner pauses before entering retry.
    conn.create_scalar_function("unixepoch", 0, FunctionFlags::SQLITE_UTF8, move |_| {
        Ok(clock.load(Ordering::SeqCst))
    })
    .unwrap();
    conn
}

fn call_retry(conn: &Connection, id: i64, sql: bool) -> rusqlite::Result<i64> {
    if sql {
        conn.query_row("SELECT honker_retry(?1, 'old', 0, 'retry')", [id], |r| {
            r.get(0)
        })
    } else {
        honker_ops::retry(conn, id, "old", 0, "retry")
    }
}

fn is_mutation(ctx: AuthContext<'_>, exhausted: bool) -> bool {
    match ctx.action {
        AuthAction::Delete { table_name } => exhausted && table_name == "_honker_live",
        AuthAction::Update { table_name, .. } => !exhausted && table_name == "_honker_live",
        _ => false,
    }
}

fn interleave(exhausted: bool, sql: bool, outer: bool) {
    let dir = std::env::temp_dir().join(format!(
        "honker-retry-owner-{}-{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jobs.db");
    let clock = Arc::new(AtomicI64::new(1000));
    let a = connect(&path, &clock);
    let b = connect(&path, &clock);
    let id = honker_ops::enqueue(
        &a,
        "q",
        "{}",
        None,
        None,
        0,
        if exhausted { 1 } else { 3 },
        None,
    )
    .unwrap();
    honker_ops::claim_batch(&a, "q", "old", 1, 5).unwrap();
    if outer {
        a.execute_batch("BEGIN").unwrap();
    }
    let fired = Arc::new(AtomicBool::new(false));
    let hit = fired.clone();
    // Whether B's interfering write committed. B uses busy_timeout=0, so it
    // fails instead of waiting when A already holds the write lock.
    let b_committed = Arc::new(AtomicBool::new(false));
    let committed = b_committed.clone();
    a.authorizer(Some(move |ctx: AuthContext<'_>| {
        if is_mutation(ctx, exhausted) && !hit.swap(true, Ordering::SeqCst) {
            // SQLite calls this while preparing retry's mutation. If B
            // commits here, A's guarded write must see B's change and leave
            // B's result alone.
            if exhausted {
                match honker_ops::cancel(&b, id) {
                    Ok(n) => {
                        assert_eq!(n, 1);
                        committed.store(true, Ordering::SeqCst);
                    }
                    Err(e) => assert!(e.to_string().contains("locked"), "{e}"),
                }
            } else {
                clock.store(1006, Ordering::SeqCst);
                match honker_ops::claim_batch(&b, "q", "new", 1, 300) {
                    Ok(claimed) => {
                        let jobs: serde_json::Value = serde_json::from_str(&claimed).unwrap();
                        assert_eq!(jobs[0]["worker_id"], "new");
                        committed.store(true, Ordering::SeqCst);
                    }
                    Err(e) => assert!(e.to_string().contains("locked"), "{e}"),
                }
            }
        }
        Authorization::Allow
    }))
    .unwrap();
    let result = call_retry(&a, id, sql);
    a.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
    assert!(
        fired.load(Ordering::SeqCst),
        "must exercise the gap before retry's mutation"
    );
    assert_eq!(
        a.is_autocommit(),
        !outer,
        "retry must preserve transaction ownership"
    );
    if outer {
        // Commit, not roll back: whatever retry wrote inside the caller's
        // transaction must be safe to keep.
        a.execute_batch("COMMIT").unwrap();
    }
    let b_won = b_committed.load(Ordering::SeqCst);
    // A deferred outer BEGIN holds no lock until retry's first write, so
    // B can always commit in that gap. Only the exhausted branch, whose
    // DELETE follows retry's own UPDATE, may find A holding the lock.
    if !exhausted {
        assert!(b_won, "the reclaim must commit before retry's first write");
    }
    // The guarantees first: B's committed result survives, and no row
    // becomes dead unless A really owned it.
    let live = honker_ops::get_job(&a, id).unwrap();
    let dead: i64 = a
        .query_row("SELECT count(*) FROM _honker_dead", [], |r| r.get(0))
        .unwrap();
    if !b_won {
        // B could not commit, so A legitimately owned the job throughout.
        assert_eq!(live, "", "A's final retry must remove the job");
        assert_eq!(dead, 1, "A's final retry must dead-letter the job");
        assert_eq!(*result.as_ref().unwrap(), 1);
    } else if exhausted {
        assert_eq!(live, "", "cancelled job must remain absent");
        assert_eq!(dead, 0, "a cancelled row must not become a dead row");
    } else {
        let row: serde_json::Value = serde_json::from_str(&live).unwrap();
        assert_eq!(row["state"], "processing", "B's claim must survive");
        assert_eq!(row["worker_id"], "new", "B's claim must survive");
        assert_eq!(row["attempts"], 2);
        assert_eq!(row["claimed_at"], 1006);
        assert_eq!(row["claim_expires_at"], 1306);
        assert_eq!(dead, 0, "a reclaimed row must not become a dead row");
    }
    // Then the report: after B's commit, A returns a miss or a lock
    // error, never success.
    if b_won {
        match &result {
            Ok(n) => assert_eq!(*n, 0, "retry must not report success over B's commit"),
            Err(e) => assert!(e.to_string().contains("locked"), "unexpected error: {e}"),
        }
    }
    drop(a);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn retry_cannot_overwrite_another_connections_reclaim() {
    for sql in [false, true] {
        for outer in [false, true] {
            interleave(false, sql, outer);
        }
    }
}

#[test]
fn final_retry_cannot_resurrect_another_connections_cancel() {
    for sql in [false, true] {
        for outer in [false, true] {
            interleave(true, sql, outer);
        }
    }
}

#[test]
fn retry_rechecks_lease_at_write_time_for_both_branches() {
    for exhausted in [false, true] {
        let clock = Arc::new(AtomicI64::new(1000));
        let a = connect(std::path::Path::new(":memory:"), &clock);
        let id = honker_ops::enqueue(
            &a,
            "q",
            "{}",
            None,
            None,
            0,
            if exhausted { 1 } else { 3 },
            None,
        )
        .unwrap();
        honker_ops::claim_batch(&a, "q", "old", 1, 5).unwrap();
        let before = honker_ops::get_job(&a, id).unwrap();
        let hit = Arc::new(AtomicBool::new(false));
        let fired = hit.clone();
        a.authorizer(Some(move |ctx: AuthContext<'_>| {
            if is_mutation(ctx, exhausted) {
                fired.store(true, Ordering::SeqCst);
                clock.store(1006, Ordering::SeqCst);
            }
            Authorization::Allow
        }))
        .unwrap();
        assert_eq!(
            call_retry(&a, id, true).unwrap(),
            0,
            "a lease that expired before the write must be a miss"
        );
        a.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
            .unwrap();
        assert!(hit.load(Ordering::SeqCst));
        assert_eq!(honker_ops::get_job(&a, id).unwrap(), before);
        let dead: i64 = a
            .query_row("SELECT count(*) FROM _honker_dead", [], |r| r.get(0))
            .unwrap();
        assert_eq!(dead, 0);
        assert!(a.is_autocommit());
    }
}

#[test]
fn failed_final_retry_preserves_callers_write_and_the_job() {
    let a = connect(
        std::path::Path::new(":memory:"),
        &Arc::new(AtomicI64::new(1000)),
    );
    let id = honker_ops::enqueue(&a, "q", "{}", None, None, 0, 1, None).unwrap();
    honker_ops::claim_batch(&a, "q", "old", 1, 300).unwrap();
    let before = honker_ops::get_job(&a, id).unwrap();
    a.execute_batch("CREATE TABLE app(x); CREATE TRIGGER reject_dead BEFORE INSERT ON _honker_dead BEGIN SELECT RAISE(ABORT, 'reject dead'); END; BEGIN; INSERT INTO app VALUES (42);").unwrap();
    let err = call_retry(&a, id, true).unwrap_err();
    assert!(err.to_string().contains("reject dead"), "{err}");
    assert!(!a.is_autocommit());
    assert_eq!(honker_ops::get_job(&a, id).unwrap(), before);
    a.execute_batch("COMMIT").unwrap();
    assert_eq!(
        a.query_row("SELECT x FROM app", [], |r| r.get::<_, i64>(0))
            .unwrap(),
        42
    );
    assert_eq!(
        a.query_row("SELECT count(*) FROM _honker_dead", [], |r| r
            .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn temp_db(tag: &str) -> (std::path::PathBuf, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "honker-retry-{tag}-{}-{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jobs.db");
    (dir, path)
}

fn open_waiting(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000;")
        .unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    conn
}

fn is_live_write(ctx: &AuthContext<'_>) -> bool {
    matches!(
        ctx.action,
        AuthAction::Delete { table_name } | AuthAction::Update { table_name, .. }
            if table_name == "_honker_live"
    )
}

/// Two workers on separate WAL connections retry their own jobs. While A
/// prepares each of its writes, B retries its job and enqueues into an
/// unrelated queue, so other commits land between every step of A's call.
/// None of that touches A's job, so A must succeed. A design that reads
/// before it writes fails here with "database is locked"
/// (SQLITE_BUSY_SNAPSHOT), which busy_timeout does not retry.
fn concurrent_commits_between_steps(exhausted: bool, sql: bool) {
    let (dir, path) = temp_db("snapshot");
    let a = open_waiting(&path);
    let b = open_waiting(&path);
    let max_attempts = if exhausted { 1 } else { 3 };
    let job_a = honker_ops::enqueue(&a, "q", "{}", None, None, 0, max_attempts, None).unwrap();
    let job_b = honker_ops::enqueue(&a, "q", "{}", None, None, 0, max_attempts, None).unwrap();
    honker_ops::claim_batch(&a, "q", "wa", 1, 300).unwrap();
    honker_ops::claim_batch(&b, "q", "wb", 1, 300).unwrap();
    let commits = Arc::new(AtomicU64::new(0));
    let seen = commits.clone();
    let b_retried = Arc::new(AtomicBool::new(false));
    let retried = b_retried.clone();
    a.authorizer(Some(move |ctx: AuthContext<'_>| {
        if is_live_write(&ctx) {
            if !retried.swap(true, Ordering::SeqCst) {
                assert_eq!(honker_ops::retry(&b, job_b, "wb", 0, "b").unwrap(), 1);
                seen.fetch_add(1, Ordering::SeqCst);
            }
            honker_ops::enqueue(&b, "other-queue", "{}", None, None, 0, 3, None).unwrap();
            seen.fetch_add(1, Ordering::SeqCst);
        }
        Authorization::Allow
    }))
    .unwrap();
    let result = if sql {
        a.query_row("SELECT honker_retry(?1, 'wa', 0, 'a')", [job_a], |r| {
            r.get::<_, i64>(0)
        })
    } else {
        honker_ops::retry(&a, job_a, "wa", 0, "a")
    };
    a.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
    assert!(
        commits.load(Ordering::SeqCst) >= 2,
        "other connections must commit during retry"
    );
    assert_eq!(
        result.expect("an unrelated commit must not fail retry"),
        1,
        "exhausted={exhausted} sql={sql}"
    );
    assert!(a.is_autocommit());
    let dead: i64 = a
        .query_row("SELECT count(*) FROM _honker_dead", [], |r| r.get(0))
        .unwrap();
    let pending: i64 = a
        .query_row(
            "SELECT count(*) FROM _honker_live WHERE queue = 'q' AND state = 'pending'",
            [],
            |r| r.get(0),
        )
        .unwrap();
    let expected = if exhausted { (2, 0) } else { (0, 2) };
    assert_eq!(
        (dead, pending),
        expected,
        "both retries must land: (dead, pending)"
    );
    drop(a);
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn retry_survives_commits_from_other_connections_between_steps() {
    for exhausted in [false, true] {
        for sql in [false, true] {
            concurrent_commits_between_steps(exhausted, sql);
        }
    }
}

/// SQLITE_BUSY_SNAPSHOT: a write tried to upgrade a read snapshot that
/// another connection's commit replaced. busy_timeout never retries it.
const BUSY_SNAPSHOT: std::ffi::c_int = 517;

fn extended_code(err: &rusqlite::Error) -> Option<std::ffi::c_int> {
    match err {
        rusqlite::Error::SqliteFailure(e, _) => Some(e.extended_code),
        _ => None,
    }
}

/// Free-running version of the test above: several threads, each with its
/// own WAL connection, claim and retry from one shared pool. Small enough
/// for CI; reviews/retry_stress.py runs the multi-process version against
/// the loadable extension.
///
/// Uses the direct API so the extended error code is visible. A plain
/// SQLITE_BUSY after busy_timeout is lock starvation under this tight loop
/// (seen on Windows CI) and is tolerated. SQLITE_BUSY_SNAPSHOT is the
/// read-then-write failure and must never happen.
#[test]
fn retry_under_contention_never_hits_a_stale_snapshot() {
    let (dir, path) = temp_db("contention");
    {
        let conn = open_waiting(&path);
        for _ in 0..32 {
            honker_ops::enqueue(&conn, "q", "{}", None, None, 0, 1_000_000, None).unwrap();
        }
    }
    let handles: Vec<_> = (0..4)
        .map(|t| {
            let path = path.clone();
            std::thread::spawn(move || {
                let conn = open_waiting(&path);
                let worker = format!("w{t}");
                let mut retried = 0;
                for _ in 0..100 {
                    std::thread::yield_now();
                    let claimed = match honker_ops::claim_batch(&conn, "q", &worker, 1, 300) {
                        Ok(claimed) => claimed,
                        Err(e) if extended_code(&e) == Some(rusqlite::ffi::SQLITE_BUSY) => {
                            continue;
                        }
                        Err(e) => panic!("claim failed: {e:?}"),
                    };
                    let jobs: serde_json::Value = serde_json::from_str(&claimed).unwrap();
                    let Some(id) = jobs[0]["id"].as_i64() else {
                        continue;
                    };
                    match honker_ops::retry(&conn, id, &worker, 0, "e") {
                        Ok(n) => {
                            assert_eq!(n, 1, "the worker still owned job {id}");
                            retried += 1;
                        }
                        Err(e) => {
                            assert_ne!(
                                extended_code(&e),
                                Some(BUSY_SNAPSHOT),
                                "retry hit a stale read snapshot: {e:?}"
                            );
                            assert_eq!(
                                extended_code(&e),
                                Some(rusqlite::ffi::SQLITE_BUSY),
                                "unexpected retry error: {e:?}"
                            );
                        }
                    }
                }
                retried
            })
        })
        .collect();
    let total: i64 = handles.into_iter().map(|h| h.join().unwrap()).sum();
    assert!(total > 0, "no retry succeeded");
    std::fs::remove_dir_all(dir).unwrap();
}

/// The pending branch is one UPDATE with no savepoint, so it works in the
/// SQL contexts that cannot open a savepoint, as it did before. A miss
/// there is a plain 0 as well.
#[test]
fn pending_retry_and_miss_work_inside_triggers_and_dml() {
    let clock = Arc::new(AtomicI64::new(1000));
    let conn = connect(std::path::Path::new(":memory:"), &clock);
    let first = honker_ops::enqueue(&conn, "q", "{}", None, None, 0, 3, None).unwrap();
    let second = honker_ops::enqueue(&conn, "q", "{}", None, None, 0, 3, None).unwrap();
    honker_ops::claim_batch(&conn, "q", "w", 2, 300).unwrap();
    conn.execute_batch(&format!(
        "CREATE TABLE app(x);
         CREATE TRIGGER t AFTER INSERT ON app WHEN new.x = 1
         BEGIN SELECT honker_retry({first}, 'w', 0, 'e'); END;
         INSERT INTO app VALUES (1);
         INSERT INTO app SELECT honker_retry({second}, 'w', 0, 'e');
         INSERT INTO app SELECT honker_retry({second}, 'w', 0, 'e');"
    ))
    .unwrap();
    for id in [first, second] {
        let row: serde_json::Value =
            serde_json::from_str(&honker_ops::get_job(&conn, id).unwrap()).unwrap();
        assert_eq!(row["state"], "pending");
    }
    let results: Vec<i64> = conn
        .prepare("SELECT x FROM app WHERE rowid > 1 ORDER BY rowid")
        .unwrap()
        .query_map([], |r| r.get(0))
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(results, vec![1, 0], "retry then a miss");
}
