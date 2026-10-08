//! Fenced ack/retry/fail/heartbeat (issue #176).
//!
//! The claim's `attempts` is its token. The fenced forms match on
//! `id + worker_id + attempts = token + state = 'processing'` with no
//! lease check, so a stale handler that shares the new holder's worker
//! id is refused, and a late handler whose job nobody reclaimed still
//! completes. The clock is a stub `unixepoch()` so leases lapse without
//! sleeping.
use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OptionalExtension};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

const W: &str = "w1";

struct Db {
    conn: Connection,
    clock: Arc<AtomicI64>,
}

impl Db {
    fn new() -> Self {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        let clock = Arc::new(AtomicI64::new(1_000));
        let c = clock.clone();
        conn.create_scalar_function("unixepoch", 0, FunctionFlags::SQLITE_UTF8, move |_| {
            Ok(c.load(Ordering::SeqCst))
        })
        .unwrap();
        Db { conn, clock }
    }

    fn advance(&self, secs: i64) {
        self.clock.fetch_add(secs, Ordering::SeqCst);
    }

    fn now(&self) -> i64 {
        self.clock.load(Ordering::SeqCst)
    }

    fn enqueue(&self, max_attempts: i64) -> i64 {
        honker_ops::enqueue(&self.conn, "q", "{}", None, None, 0, max_attempts, None).unwrap()
    }

    /// Claim one job with a 5 s lease; returns (id, attempts).
    fn claim(&self, worker: &str) -> Option<(i64, i64)> {
        let raw = honker_ops::claim_batch(&self.conn, "q", worker, 1, 5).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        v.as_array()
            .unwrap()
            .first()
            .map(|j| (j["id"].as_i64().unwrap(), j["attempts"].as_i64().unwrap()))
    }

    /// (state, worker_id, attempts, claim_expires_at) of the live row.
    fn live(&self, id: i64) -> Option<(String, Option<String>, i64, Option<i64>)> {
        self.conn
            .query_row(
                "SELECT state, worker_id, attempts, claim_expires_at
                 FROM _honker_live WHERE id = ?1",
                [id],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .optional()
            .unwrap()
    }

    fn dead(&self, id: i64) -> Option<String> {
        self.conn
            .query_row(
                "SELECT last_error FROM _honker_dead WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
    }

    fn sql(&self, sql: &str, params: impl rusqlite::Params) -> i64 {
        self.conn.query_row(sql, params, |r| r.get(0)).unwrap()
    }

    /// A job claimed by W at attempt 1, whose lease then lapsed and W
    /// reclaimed it at attempt 2. Returns the id.
    fn reclaimed_by_same_worker(&self) -> i64 {
        let id = self.enqueue(5);
        assert_eq!(self.claim(W), Some((id, 1)));
        self.advance(6);
        assert_eq!(self.claim(W), Some((id, 2)));
        id
    }
}

/// Each fenced op through both the Rust API and the SQL function.
#[derive(Clone, Copy, Debug)]
enum Op {
    Ack,
    Retry,
    Fail,
    Heartbeat,
}

const OPS: [Op; 4] = [Op::Ack, Op::Retry, Op::Fail, Op::Heartbeat];

fn fenced(db: &Db, op: Op, id: i64, attempt: i64, via_sql: bool) -> i64 {
    let c = &db.conn;
    if via_sql {
        let p = rusqlite::params![id, W, attempt];
        return match op {
            Op::Ack => db.sql("SELECT honker_ack(?1, ?2, ?3)", p),
            Op::Retry => db.sql("SELECT honker_retry(?1, ?2, 0, 'stale', ?3)", p),
            Op::Fail => db.sql("SELECT honker_fail(?1, ?2, 'stale', ?3)", p),
            Op::Heartbeat => db.sql("SELECT honker_heartbeat(?1, ?2, 30, ?3)", p),
        };
    }
    match op {
        Op::Ack => honker_ops::ack_fenced(c, id, W, attempt),
        Op::Retry => honker_ops::retry_fenced(c, id, W, 0, "stale", attempt),
        Op::Fail => honker_ops::fail_fenced(c, id, W, "stale", attempt),
        Op::Heartbeat => honker_ops::heartbeat_fenced(c, id, W, 30, attempt),
    }
    .unwrap()
}

#[test]
fn stale_same_worker_fenced_calls_are_refused_and_leave_the_new_claim() {
    for via_sql in [false, true] {
        for op in OPS {
            let db = Db::new();
            let id = db.reclaimed_by_same_worker();
            let before = db.live(id);
            assert_eq!(
                fenced(&db, op, id, 1, via_sql),
                0,
                "{op:?} sql={via_sql}: stale attempt 1 acted on attempt 2"
            );
            assert_eq!(db.live(id), before, "{op:?} sql={via_sql}: row changed");
            assert_eq!(db.dead(id), None, "{op:?} sql={via_sql}");
            // The new holder still owns it and can finish it.
            assert_eq!(fenced(&db, Op::Ack, id, 2, via_sql), 1, "{op:?}");
            assert_eq!(db.live(id), None);
        }
    }
}

#[test]
fn the_unfenced_forms_still_let_a_stale_same_worker_ack_through() {
    // Documents why the fenced forms exist: the old arity only checks
    // worker_id and the lease, which the reclaim refreshed.
    let db = Db::new();
    let id = db.reclaimed_by_same_worker();
    assert_eq!(
        db.sql("SELECT honker_ack(?1, ?2)", rusqlite::params![id, W]),
        1
    );
    assert_eq!(db.live(id), None);
}

#[test]
fn a_late_but_unreclaimed_fenced_ack_succeeds() {
    for via_sql in [false, true] {
        let db = Db::new();
        let id = db.enqueue(5);
        assert_eq!(db.claim(W), Some((id, 1)));
        db.advance(60); // lease long gone, nobody reclaimed
        // The unfenced form refuses: it needs a live lease.
        assert_eq!(honker_ops::ack(&db.conn, id, W).unwrap(), 0);
        assert_eq!(fenced(&db, Op::Ack, id, 1, via_sql), 1);
        assert_eq!(db.live(id), None);
        assert_eq!(db.dead(id), None);
        // It does not run again.
        assert_eq!(db.claim("w2"), None);
    }
}

#[test]
fn a_late_but_unreclaimed_fenced_heartbeat_revives_the_lease() {
    let db = Db::new();
    let id = db.enqueue(5);
    assert_eq!(db.claim(W), Some((id, 1)));
    db.advance(60);
    assert_eq!(honker_ops::heartbeat(&db.conn, id, W, 30).unwrap(), 0);
    assert_eq!(
        honker_ops::heartbeat_fenced(&db.conn, id, W, 30, 1).unwrap(),
        1
    );
    let (state, worker, attempts, exp) = db.live(id).unwrap();
    assert_eq!(
        (state.as_str(), worker.as_deref(), attempts, exp),
        ("processing", Some(W), 1, Some(db.now() + 30))
    );
    // Nobody can reclaim it now, and the holder can still ack.
    assert_eq!(db.claim("w2"), None);
    assert_eq!(honker_ops::ack_fenced(&db.conn, id, W, 1).unwrap(), 1);
}

#[test]
fn late_fenced_retry_and_fail_succeed_when_unreclaimed() {
    let db = Db::new();
    let id = db.enqueue(5);
    db.claim(W).unwrap();
    db.advance(60);
    assert_eq!(
        honker_ops::retry_fenced(&db.conn, id, W, 0, "e", 1).unwrap(),
        1
    );
    let (state, worker, attempts, exp) = db.live(id).unwrap();
    assert_eq!(
        (state.as_str(), worker, attempts, exp),
        ("pending", None, 1, None)
    );

    assert_eq!(db.claim(W), Some((id, 2)));
    db.advance(60);
    assert_eq!(
        honker_ops::fail_fenced(&db.conn, id, W, "boom", 2).unwrap(),
        1
    );
    assert_eq!(db.live(id), None);
    assert_eq!(db.dead(id).as_deref(), Some("boom"));
}

#[test]
fn fenced_retry_dead_letters_the_last_attempt_and_refuses_a_wrong_token() {
    let db = Db::new();
    let id = db.enqueue(1);
    assert_eq!(db.claim(W), Some((id, 1)));
    // Wrong token: neither branch runs.
    assert_eq!(
        honker_ops::retry_fenced(&db.conn, id, W, 0, "e", 2).unwrap(),
        0
    );
    assert_eq!(db.live(id).unwrap().0, "processing");
    assert_eq!(db.dead(id), None);
    // Right token on an exhausted job: dead-lettered, even after the lease.
    db.advance(60);
    assert_eq!(
        db.sql(
            "SELECT honker_retry(?1, ?2, 0, 'last', 1)",
            rusqlite::params![id, W]
        ),
        1
    );
    assert_eq!(db.live(id), None);
    assert_eq!(db.dead(id).as_deref(), Some("last"));
}

#[test]
fn fenced_calls_miss_on_other_workers_pending_rows_and_removed_rows() {
    let db = Db::new();
    let id = db.enqueue(5);
    assert_eq!(db.claim(W), Some((id, 1)));
    // Right token, wrong worker.
    assert_eq!(honker_ops::ack_fenced(&db.conn, id, "w2", 1).unwrap(), 0);
    // Back to pending: attempts is still 1, but the row is not claimed.
    assert_eq!(
        honker_ops::retry_fenced(&db.conn, id, W, 0, "e", 1).unwrap(),
        1
    );
    db.conn
        .execute(
            "UPDATE _honker_live SET worker_id = ?1 WHERE id = ?2",
            rusqlite::params![W, id],
        )
        .unwrap();
    for op in OPS {
        assert_eq!(fenced(&db, op, id, 1, false), 0, "{op:?} on a pending row");
    }
    // Cancelled: gone.
    assert_eq!(honker_ops::cancel(&db.conn, id).unwrap(), 1);
    for op in OPS {
        assert_eq!(fenced(&db, op, id, 1, true), 0, "{op:?} on a cancelled row");
    }
}

#[test]
fn ack_batch_fences_pairs_and_keeps_plain_ids_unfenced() {
    let db = Db::new();
    let stale = db.reclaimed_by_same_worker(); // attempts = 2
    let late = db.enqueue(5);
    assert_eq!(db.claim(W), Some((late, 1)));
    let plain = db.enqueue(5);
    assert_eq!(db.claim(W), Some((plain, 1)));

    // Stale pair refused; nothing else touched.
    let json = format!("[[{stale},1]]");
    assert_eq!(honker_ops::ack_batch(&db.conn, &json, W).unwrap(), 0);
    assert!(db.live(stale).is_some());

    // Mixed: a current pair plus a plain id, both with live leases.
    let json = format!("[[{stale},2],{plain}]");
    assert_eq!(honker_ops::ack_batch(&db.conn, &json, W).unwrap(), 2);
    assert_eq!((db.live(stale), db.live(plain)), (None, None));

    // Another worker's claim, then let every lease lapse with nobody
    // reclaiming. A pair still acks (no lease check); a plain id does not.
    let other = db.enqueue(5);
    assert_eq!(db.claim("w2"), Some((other, 1)));
    db.advance(60);
    assert_eq!(
        db.sql(
            "SELECT honker_ack_batch(?1, ?2)",
            rusqlite::params![format!("[{other}]"), "w2"]
        ),
        0,
        "plain id keeps the lease check"
    );
    assert_eq!(
        db.sql(
            "SELECT honker_ack_batch(?1, ?2)",
            rusqlite::params![format!("[[{late},1],[{other},1]]"), W]
        ),
        1,
        "pair acks a late unreclaimed claim, but only for its own worker"
    );
    assert_eq!(db.live(late), None);
    assert!(db.live(other).is_some());
}

#[test]
fn fenced_attempt_accepts_a_whole_real() {
    // better-sqlite3 binds every JS number as REAL.
    let db = Db::new();
    let id = db.enqueue(5);
    db.claim(W).unwrap();
    assert_eq!(
        db.sql("SELECT honker_ack(?1, ?2, 1.0)", rusqlite::params![id, W]),
        1
    );
    let err = db
        .conn
        .query_row("SELECT honker_ack(1, 'w', 1.5)", [], |r| r.get::<_, i64>(0))
        .unwrap_err();
    assert!(err.to_string().contains("fractional"), "{err}");
}
