//! `scheduler_tick` is atomic on its own (#173): two ticks never enqueue
//! the same boundary, a commit from another connection never fails a
//! tick with a stale snapshot, and a failed tick leaves nothing behind.
//!
//! Two connections share one WAL file. The clock is a stub
//! `unixepoch()`, and A's authorizer runs B's interfering call at an
//! exact point inside A's tick, so every interleaving is deterministic.
use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::Connection;
use rusqlite::functions::FunctionFlags;
use rusqlite::hooks::{AuthAction, AuthContext, Authorization};
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

static NEXT_DB: AtomicU64 = AtomicU64::new(0);

/// `@every 1s` registered at clock 1000 first fires at 1001.
const FIRST: i64 = 1001;

struct Pair {
    dir: std::path::PathBuf,
    a: Connection,
    b: Arc<Mutex<Connection>>,
}

impl Drop for Pair {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn connect(path: &std::path::Path) -> Connection {
    let conn = Connection::open(path).unwrap();
    // busy_timeout=0: B fails at once instead of waiting when A holds
    // the write lock, so the test never blocks inside A's callback.
    conn.execute_batch("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=0;")
        .unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    conn.create_scalar_function("unixepoch", 0, FunctionFlags::SQLITE_UTF8, |_| Ok(1000))
        .unwrap();
    conn
}

fn pair() -> Pair {
    let dir = std::env::temp_dir().join(format!(
        "honker-tick-{}-{}",
        std::process::id(),
        NEXT_DB.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("jobs.db");
    let a = connect(&path);
    let b = Arc::new(Mutex::new(connect(&path)));
    Pair { dir, a, b }
}

fn register(conn: &Connection, name: &str, queue: &str) {
    honker_ops::scheduler_register(conn, name, queue, "@every 1s", "{}", 0, None, 3).unwrap();
}

fn tick(conn: &Connection, at: i64, sql: bool) -> rusqlite::Result<String> {
    if sql {
        conn.query_row("SELECT honker_scheduler_tick(?1)", [at], |r| r.get(0))
    } else {
        honker_ops::scheduler_tick(conn, at)
    }
}

/// `(name, fire_at)` of every fire in a tick's result.
fn fires(json: &str) -> Vec<(String, i64)> {
    let v: serde_json::Value = serde_json::from_str(json).unwrap();
    v.as_array()
        .unwrap()
        .iter()
        .map(|f| {
            (
                f["name"].as_str().unwrap().to_string(),
                f["fire_at"].as_i64().unwrap(),
            )
        })
        .collect()
}

fn jobs(conn: &Connection, queue: &str) -> i64 {
    conn.query_row(
        "SELECT count(*) FROM _honker_live WHERE queue = ?1",
        [queue],
        |r| r.get(0),
    )
    .unwrap()
}

fn next_fire_at(conn: &Connection, name: &str) -> i64 {
    conn.query_row(
        "SELECT next_fire_at FROM _honker_scheduler_tasks WHERE name = ?1",
        [name],
        |r| r.get(0),
    )
    .unwrap()
}

fn assert_locked(e: &rusqlite::Error) {
    assert!(e.to_string().contains("locked"), "unexpected error: {e}");
}

fn clear_hook(conn: &Connection) {
    conn.authorizer(None::<fn(AuthContext<'_>) -> Authorization>)
        .unwrap();
}

/// B ticks while A's tick is about to enqueue its first job. A must
/// succeed, B must find the lock taken, and each boundary must be
/// enqueued exactly once. Reading the due tasks before taking the lock
/// fails this: in autocommit both ticks enqueue the boundaries; with a
/// read snapshot open, A fails with SQLITE_BUSY_SNAPSHOT.
fn overlapping_tick(sql: bool, outer: bool) {
    let p = pair();
    register(&p.a, "t", "q");
    let at = FIRST + 4;
    if outer {
        p.a.execute_batch("BEGIN").unwrap();
    }
    let b_result: Arc<Mutex<Option<rusqlite::Result<String>>>> = Arc::new(Mutex::new(None));
    let (b, slot) = (p.b.clone(), b_result.clone());
    p.a.authorizer(Some(move |ctx: AuthContext<'_>| {
        if let AuthAction::Insert { table_name } = ctx.action {
            let mut slot = slot.lock().unwrap();
            if table_name == "_honker_live" && slot.is_none() {
                *slot = Some(tick(&b.lock().unwrap(), at, sql));
            }
        }
        Authorization::Allow
    }))
    .unwrap();
    let a_result = tick(&p.a, at, sql);
    clear_hook(&p.a);
    if outer {
        p.a.execute_batch("COMMIT").unwrap();
    }

    let a_fires = fires(&a_result.expect("the tick already under way must not fail"));
    let want: Vec<(String, i64)> = (FIRST..=at).map(|t| ("t".to_string(), t)).collect();
    assert_eq!(a_fires, want, "A fires every due boundary once");
    match b_result.lock().unwrap().take() {
        Some(Ok(json)) => panic!("B's tick ran inside A's tick and fired {json}"),
        Some(Err(e)) => assert_locked(&e),
        None => panic!("the hook never ran B's tick"),
    }
    let b = p.b.lock().unwrap();
    assert_eq!(tick(&b, at, sql).unwrap(), "[]", "nothing is left to fire");
    assert_eq!(jobs(&b, "q"), at - FIRST + 1, "one job per boundary");
    assert_eq!(next_fire_at(&b, "t"), at + 1);
}

#[test]
fn overlapping_ticks_enqueue_each_boundary_once() {
    for sql in [false, true] {
        for outer in [false, true] {
            overlapping_tick(sql, outer);
        }
    }
}

/// B commits a write at every step of A's tick where it can: every
/// statement A prepares gives B a chance. Inside a deferred BEGIN a read
/// before the tick's first write would pin a snapshot, and B's commit
/// would then fail A's write with "database is locked".
fn commit_during_tick(sql: bool, outer: bool) {
    let p = pair();
    register(&p.a, "t", "q");
    let at = FIRST + 2;
    if outer {
        p.a.execute_batch("BEGIN").unwrap();
    }
    let committed = Arc::new(AtomicI64::new(0));
    let (b, count) = (p.b.clone(), committed.clone());
    p.a.authorizer(Some(move |_: AuthContext<'_>| {
        let b = b.lock().unwrap();
        match honker_ops::enqueue(&b, "other", "{}", None, None, 0, 3, None) {
            Ok(_) => {
                count.fetch_add(1, Ordering::SeqCst);
            }
            Err(e) => assert_locked(&e),
        }
        Authorization::Allow
    }))
    .unwrap();
    let a_result = tick(&p.a, at, sql);
    clear_hook(&p.a);
    if outer {
        p.a.execute_batch("COMMIT").unwrap();
    }
    let a_fires = fires(&a_result.expect("a commit from B must not fail A's tick"));
    assert_eq!(a_fires.len() as i64, at - FIRST + 1);
    assert!(
        committed.load(Ordering::SeqCst) > 0,
        "B must commit at least once while A's tick runs"
    );
    assert_eq!(jobs(&p.a, "q"), at - FIRST + 1);
}

#[test]
fn a_commit_from_another_connection_does_not_fail_the_tick() {
    for sql in [false, true] {
        for outer in [false, true] {
            commit_during_tick(sql, outer);
        }
    }
}

/// A trigger fails the third enqueue of task `t`. Task `a` comes first
/// (fires are in name order) and enqueues fine. The failed tick must
/// leave no job from either task and advance neither, and the next tick
/// must fire every boundary exactly once.
fn failed_tick(sql: bool, outer: bool) {
    let p = pair();
    register(&p.a, "a", "ok");
    register(&p.a, "t", "q");
    let at = FIRST + 4;
    p.a.execute_batch(
        "CREATE TRIGGER boom BEFORE INSERT ON _honker_live
           WHEN NEW.queue = 'q'
            AND (SELECT count(*) FROM _honker_live WHERE queue = 'q') >= 2
         BEGIN SELECT RAISE(ABORT, 'boom'); END",
    )
    .unwrap();
    if outer {
        p.a.execute_batch("BEGIN").unwrap();
        // Work of the caller's that must survive the tick's failure.
        honker_ops::enqueue(&p.a, "mine", "{}", None, None, 0, 3, None).unwrap();
    }
    let err = tick(&p.a, at, sql).expect_err("the trigger fails the tick");
    assert!(err.to_string().contains("boom"), "{err}");
    if outer {
        assert!(!p.a.is_autocommit(), "the caller's transaction stays open");
        p.a.execute_batch("COMMIT").unwrap();
        assert_eq!(jobs(&p.a, "mine"), 1, "the caller's own work survives");
    }
    assert!(p.a.is_autocommit());
    assert_eq!(
        jobs(&p.a, "ok"),
        0,
        "no job from the task before the failure"
    );
    assert_eq!(jobs(&p.a, "q"), 0, "no orphan job from the failing task");
    assert_eq!(
        next_fire_at(&p.a, "a"),
        FIRST,
        "no advance without its jobs"
    );
    assert_eq!(
        next_fire_at(&p.a, "t"),
        FIRST,
        "no advance without its jobs"
    );

    p.a.execute_batch("DROP TRIGGER boom").unwrap();
    let mut got = fires(&tick(&p.a, at, sql).unwrap());
    got.sort();
    let mut want: Vec<(String, i64)> = Vec::new();
    for name in ["a", "t"] {
        want.extend((FIRST..=at).map(|t| (name.to_string(), t)));
    }
    assert_eq!(got, want, "the retry fires every boundary exactly once");
    assert_eq!(jobs(&p.a, "ok"), at - FIRST + 1);
    assert_eq!(jobs(&p.a, "q"), at - FIRST + 1);
}

#[test]
fn a_failed_tick_leaves_no_job_and_no_advance() {
    for sql in [false, true] {
        for outer in [false, true] {
            failed_tick(sql, outer);
        }
    }
}

#[test]
fn catch_up_is_capped_and_skips_to_after_now() {
    let p = pair();
    register(&p.a, "t", "q");
    let at = FIRST + 99;
    let got = fires(&tick(&p.a, at, false).unwrap());
    let cap = honker_ops::SCHEDULER_MAX_CATCHUP_FIRES;
    let want: Vec<(String, i64)> = (FIRST..FIRST + cap).map(|t| ("t".to_string(), t)).collect();
    assert_eq!(got, want);
    assert_eq!(jobs(&p.a, "q"), cap);
    assert_eq!(next_fire_at(&p.a, "t"), at + 1, "skipped to after now");
    // Every job of one tick shares the tick's single clock reading.
    let run_ats: Vec<i64> =
        p.a.prepare("SELECT DISTINCT run_at FROM _honker_live WHERE queue = 'q'")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
    assert_eq!(run_ats, vec![1000]);
}

/// An idle tick takes the write lock but writes no page, so it does not
/// wake every `data_version` watcher.
#[test]
fn an_idle_tick_does_not_change_data_version() {
    let p = pair();
    register(&p.a, "t", "q");
    let b = p.b.lock().unwrap();
    let version = |c: &Connection| -> i64 {
        c.query_row("PRAGMA data_version", [], |r| r.get(0))
            .unwrap()
    };
    let before = version(&b);
    assert_eq!(tick(&p.a, FIRST - 1, true).unwrap(), "[]");
    assert_eq!(
        version(&b),
        before,
        "an idle tick must not look like a commit"
    );
    tick(&p.a, FIRST, true).unwrap();
    assert_ne!(version(&b), before, "a tick that fires is a commit");
}
