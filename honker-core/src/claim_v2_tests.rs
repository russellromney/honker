//! Claim v2: the `scheduled` state, the ready index, bounded
//! housekeeping inside the claim, eager expiry (#177) and the bootstrap
//! migration from the older schema.
//!
//! The clock is a stub `unixepoch()` so deadlines pass without sleeping.
use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, OptionalExtension};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

struct Db {
    conn: Connection,
    clock: Arc<AtomicI64>,
}

impl Db {
    fn new() -> Self {
        Self::with_clock(false)
    }

    /// `ticking`: every `unixepoch()` call returns the clock and then
    /// advances it by one second.
    fn with_clock(ticking: bool) -> Self {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        let clock = Arc::new(AtomicI64::new(1_000));
        let c = clock.clone();
        conn.create_scalar_function("unixepoch", 0, FunctionFlags::SQLITE_UTF8, move |_| {
            Ok(if ticking {
                c.fetch_add(1, Ordering::SeqCst)
            } else {
                c.load(Ordering::SeqCst)
            })
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

    fn enqueue(&self, delay: Option<i64>, priority: i64, expires: Option<i64>) -> i64 {
        honker_ops::enqueue(&self.conn, "q", "{}", None, delay, priority, 3, expires).unwrap()
    }

    fn enqueue_at(&self, run_at: i64, priority: i64) -> i64 {
        honker_ops::enqueue(&self.conn, "q", "{}", Some(run_at), None, priority, 3, None).unwrap()
    }

    /// Claim up to `n` jobs; returns (id, attempts) pairs.
    fn claim_n(&self, worker: &str, n: i64, lease: i64) -> Vec<(i64, i64)> {
        let raw = honker_ops::claim_batch(&self.conn, "q", worker, n, lease).unwrap();
        let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
        v.as_array()
            .unwrap()
            .iter()
            .map(|j| (j["id"].as_i64().unwrap(), j["attempts"].as_i64().unwrap()))
            .collect()
    }

    fn claim(&self, worker: &str, lease: i64) -> Option<(i64, i64)> {
        self.claim_n(worker, 1, lease).into_iter().next()
    }

    fn state(&self, id: i64) -> Option<String> {
        self.conn
            .query_row("SELECT state FROM _honker_live WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .optional()
            .unwrap()
    }

    fn dead_error(&self, id: i64) -> Option<String> {
        self.conn
            .query_row(
                "SELECT last_error FROM _honker_dead WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .optional()
            .unwrap()
    }

    fn count(&self, sql: &str) -> i64 {
        self.conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    fn next_claim_at(&self) -> i64 {
        honker_ops::queue_next_claim_at(&self.conn, "q").unwrap()
    }
}

// ---------------------------------------------------------------- states

#[test]
fn enqueue_writes_scheduled_only_for_future_run_at() {
    let db = Db::new();
    let now = db.now();
    let due = db.enqueue(None, 0, None);
    let zero_delay = db.enqueue(Some(0), 0, None);
    let past = db.enqueue_at(now - 5, 0);
    let at_now = db.enqueue_at(now, 0);
    let delayed = db.enqueue(Some(1), 0, None);
    let future = db.enqueue_at(now + 60, 0);
    for id in [due, zero_delay, past, at_now] {
        assert_eq!(db.state(id).as_deref(), Some("pending"), "job {id}");
    }
    for id in [delayed, future] {
        assert_eq!(db.state(id).as_deref(), Some("scheduled"), "job {id}");
    }
}

#[test]
fn a_scheduled_job_is_promoted_and_claimed_once_due() {
    let db = Db::new();
    let id = db.enqueue(Some(5), 0, None);
    db.advance(4);
    assert_eq!(db.claim("w", 30), None, "not due yet");
    assert_eq!(db.state(id).as_deref(), Some("scheduled"));
    db.advance(1);
    assert_eq!(db.claim("w", 30), Some((id, 1)));
    assert_eq!(db.state(id).as_deref(), Some("processing"));
}

#[test]
fn retry_writes_scheduled_for_a_delay_and_pending_without_one() {
    let db = Db::new();
    let a = db.enqueue(None, 0, None);
    let b = db.enqueue(None, 0, None);
    let (ca, aa) = db.claim("w", 30).unwrap();
    let (cb, _) = db.claim("w", 30).unwrap();
    assert_eq!((ca, cb), (a, b));
    // Fenced with a delay, unfenced without.
    assert_eq!(
        honker_ops::retry_fenced(&db.conn, a, "w", 10, "later", aa).unwrap(),
        1
    );
    assert_eq!(honker_ops::retry(&db.conn, b, "w", 0, "now").unwrap(), 1);
    assert_eq!(db.state(a).as_deref(), Some("scheduled"));
    assert_eq!(db.state(b).as_deref(), Some("pending"));
    assert_eq!(db.claim("w", 30), Some((b, 2)));
    assert_eq!(db.claim("w", 30), None, "a is 10 s out");
    db.advance(10);
    assert_eq!(db.claim("w", 30), Some((a, 2)));
}

#[test]
fn cancel_accepts_scheduled_rows_at_both_arities() {
    let db = Db::new();
    let a = db.enqueue(Some(60), 0, None);
    let b = db.enqueue(Some(60), 0, None);
    assert_eq!(honker_ops::cancel(&db.conn, a).unwrap(), 1);
    assert_eq!(
        honker_ops::cancel_in_queue(&db.conn, "other", b).unwrap(),
        0
    );
    assert_eq!(honker_ops::cancel_in_queue(&db.conn, "q", b).unwrap(), 1);
    assert_eq!(db.count("SELECT count(*) FROM _honker_live"), 0);
    assert_eq!(db.count("SELECT count(*) FROM _honker_dead"), 0);
}

#[test]
fn get_job_reports_scheduled() {
    let db = Db::new();
    let id = db.enqueue(Some(60), 0, None);
    let raw = honker_ops::get_job(&db.conn, id).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v["state"], "scheduled");
}

// -------------------------------------------------------------- ordering

/// priority DESC, run_at, id across a due pending row, a promoted
/// scheduled row and a reclaimed lapsed lease.
#[test]
fn claim_order_holds_across_promotion_and_reclaim() {
    let db = Db::new();
    let now = db.now();
    let a = db.enqueue_at(now, 0); // pending, prio 0, run_at now
    let b = db.enqueue_at(now - 1, 0); // pending, prio 0, earlier run_at
    let c = db.enqueue(Some(3), 5, None); // scheduled, prio 5, run_at now+3
    let d = db.enqueue_at(now, 5); // claimed below, lease lapses
    assert_eq!(db.claim("w0", 1), Some((d, 1)));
    let e = db.enqueue_at(now, 5); // pending, prio 5, same run_at as d, later id
    db.advance(3);
    let order: Vec<i64> = (0..5).map(|_| db.claim("w1", 60).unwrap().0).collect();
    assert_eq!(order, vec![d, e, c, b, a]);
    assert_eq!(db.claim("w1", 60), None);
}

// ---------------------------------------------------------------- expiry

/// Issue #177: a job that expires while its holder is gone (the lease
/// lapsed) moved nowhere and stayed `processing` forever.
#[test]
fn an_expired_job_with_a_lapsed_lease_is_dead_lettered_by_the_next_claim() {
    let db = Db::new();
    let id = db.enqueue(None, 0, Some(1));
    assert_eq!(db.claim("w", 1), Some((id, 1)));
    db.advance(3); // lease (now+1) and expiry (now+1) both passed
    assert_eq!(db.claim("w2", 1), None);
    assert_eq!(db.state(id), None);
    assert_eq!(db.dead_error(id).as_deref(), Some("expired"));
}

#[test]
fn sweep_expired_takes_lapsed_processing_rows_too() {
    let db = Db::new();
    let held = db.enqueue(None, 0, Some(1));
    assert_eq!(db.claim("w", 1), Some((held, 1)));
    let sched = db.enqueue(Some(5), 0, Some(2));
    let pend = db.enqueue(None, 0, Some(1));
    db.advance(3);
    assert_eq!(honker_ops::sweep_expired(&db.conn, "q").unwrap(), 3);
    for id in [held, sched, pend] {
        assert_eq!(db.dead_error(id).as_deref(), Some("expired"), "job {id}");
    }
}

/// The owner of a valid lease keeps the job even past `expires_at`, and
/// its fenced ack still works.
#[test]
fn an_expired_job_with_a_valid_lease_is_left_to_its_owner() {
    let db = Db::new();
    let id = db.enqueue(None, 0, Some(1));
    let (_, att) = db.claim("w", 60).unwrap();
    db.advance(5);
    assert_eq!(db.claim("w2", 60), None);
    assert_eq!(honker_ops::sweep_expired(&db.conn, "q").unwrap(), 0);
    assert_eq!(db.state(id).as_deref(), Some("processing"));
    assert_eq!(honker_ops::ack_fenced(&db.conn, id, "w", att).unwrap(), 1);
    assert_eq!(db.count("SELECT count(*) FROM _honker_dead"), 0);
}

/// Lapsed leases stay `processing` until a claim takes them, so a late
/// holder's fenced ack still succeeds when the claim picked other work.
#[test]
fn a_lapsed_lease_stays_processing_until_reclaimed() {
    let db = Db::new();
    let late = db.enqueue(None, 0, None);
    let (_, att) = db.claim("w", 1).unwrap();
    let urgent = db.enqueue(None, 9, None);
    db.advance(5);
    assert_eq!(db.claim("w2", 60), Some((urgent, 1)));
    assert_eq!(db.state(late).as_deref(), Some("processing"));
    assert_eq!(honker_ops::ack_fenced(&db.conn, late, "w", att).unwrap(), 1);
    assert_eq!(db.state(late), None);
}

// ------------------------------------------------------- attempt budget

#[test]
fn an_exhausted_lapsed_lease_is_dead_lettered_and_a_live_one_reclaimed() {
    let db = Db::new();
    let one = honker_ops::enqueue(&db.conn, "q", "{}", None, None, 0, 1, None).unwrap();
    let two = honker_ops::enqueue(&db.conn, "q", "{}", None, None, 0, 2, None).unwrap();
    assert_eq!(db.claim_n("w", 2, 1).len(), 2);
    db.advance(3);
    assert_eq!(db.claim("w2", 60), Some((two, 2)));
    assert_eq!(db.state(one), None);
    assert_eq!(db.dead_error(one).as_deref(), Some("max attempts exceeded"));
}

#[test]
fn max_attempts_below_one_is_rejected_everywhere() {
    let db = Db::new();
    for m in [0, -1] {
        let err = db
            .conn
            .query_row(
                "SELECT honker_enqueue('q', '{}', NULL, NULL, 0, ?1, NULL)",
                [m],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("honker_enqueue: max_attempts must be at least 1"),
            "{err}"
        );
        let err = db
            .conn
            .query_row(
                "SELECT honker_scheduler_register('t', 'q', '@every 1m', '{}', 0, NULL, ?1)",
                [m],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_err();
        assert!(
            err.to_string()
                .contains("honker_scheduler_register: max_attempts must be at least 1"),
            "{err}"
        );
    }
    assert_eq!(db.count("SELECT count(*) FROM _honker_live"), 0);
    assert_eq!(db.count("SELECT count(*) FROM _honker_scheduler_tasks"), 0);

    honker_ops::scheduler_register(&db.conn, "t", "q", "@every 1m", "{}", 0, None, 4).unwrap();
    let err = honker_ops::scheduler_update(&db.conn, "t", None, None, None, None, Some(Some(0)))
        .unwrap_err();
    assert!(
        err.to_string()
            .contains("honker_scheduler_update: max_attempts must be at least 1"),
        "{err}"
    );
    assert_eq!(
        db.count("SELECT max_attempts FROM _honker_scheduler_tasks"),
        4
    );
    assert_eq!(
        honker_ops::scheduler_update(&db.conn, "t", None, None, None, None, Some(None)).unwrap(),
        1
    );
    assert_eq!(
        db.count("SELECT max_attempts FROM _honker_scheduler_tasks"),
        3
    );
}

// ------------------------------------------------------- next_claim_at

#[test]
fn queue_next_claim_at_tracks_scheduled_rows_and_leases() {
    let db = Db::new();
    let now = db.now();
    assert_eq!(db.next_claim_at(), 0);
    db.enqueue(Some(30), 0, None);
    assert_eq!(db.next_claim_at(), now + 30);
    // A scheduled row that expires first never becomes claimable.
    db.enqueue(Some(10), 0, Some(0));
    assert_eq!(db.next_claim_at(), now + 30);
    let held = db.enqueue(None, 0, None);
    assert_eq!(
        db.next_claim_at(),
        now,
        "a due pending row is claimable now"
    );
    assert_eq!(db.claim("w", 5), Some((held, 1)));
    assert_eq!(db.next_claim_at(), now + 6, "reclaim needs the lease < now");
    // Past run_at but not promoted yet: report now, never the past.
    db.advance(40);
    assert_eq!(db.next_claim_at(), db.now());
}

#[test]
fn queue_next_claim_at_ignores_an_exhausted_lease() {
    let db = Db::new();
    honker_ops::enqueue(&db.conn, "q", "{}", None, None, 0, 1, None).unwrap();
    assert!(db.claim("w", 5).is_some());
    assert_eq!(db.next_claim_at(), 0);
}

// -------------------------------------------------- bounded housekeeping

#[test]
fn each_housekeeping_step_is_capped_per_claim() {
    let db = Db::new();
    let k = honker_ops::CLAIM_HOUSEKEEPING_LIMIT;
    let extra = 300;
    db.conn.execute_batch("BEGIN").unwrap();
    for _ in 0..(k + extra) {
        db.enqueue(Some(5), 0, None);
    }
    for _ in 0..(k + extra) {
        honker_ops::enqueue(&db.conn, "x", "{}", None, None, 0, 3, Some(1)).unwrap();
    }
    db.conn.execute_batch("COMMIT").unwrap();
    db.advance(5);
    assert_eq!(db.claim_n("w", 1, 60).len(), 1);
    let scheduled =
        db.count("SELECT count(*) FROM _honker_live WHERE queue='q' AND state='scheduled'");
    assert_eq!(scheduled, extra, "one claim promotes at most K rows");
    assert_eq!(db.claim_n("w", 1, 60).len(), 1);
    assert_eq!(
        db.count("SELECT count(*) FROM _honker_live WHERE queue='q' AND state='scheduled'"),
        0
    );

    let raw = honker_ops::claim_batch(&db.conn, "x", "w", 1, 60).unwrap();
    assert_eq!(raw, "[]");
    assert_eq!(
        db.count("SELECT count(*) FROM _honker_dead WHERE queue='x'"),
        k,
        "one claim expires at most K rows"
    );
    honker_ops::claim_batch(&db.conn, "x", "w", 1, 60).unwrap();
    assert_eq!(
        db.count("SELECT count(*) FROM _honker_dead WHERE queue='x'"),
        k + extra
    );
}

// ------------------------------------------------------------ bound now

/// Every decision in one claim uses one clock reading. With a clock that
/// advances on every `unixepoch()` call, a job that expires one second
/// after the claim starts is claimed (and stamped with that same
/// instant), not expired by a later statement of the same claim.
#[test]
fn one_claim_reads_the_clock_once() {
    let db = Db::with_clock(true);
    let id = db.enqueue(None, 0, None);
    // The claim's single reading will be `start`; the job expires one
    // tick later. (A plain UPDATE reads no clock.)
    let start = db.now();
    db.conn
        .execute(
            "UPDATE _honker_live SET expires_at = ?1 WHERE id = ?2",
            [start + 1, id],
        )
        .unwrap();
    let raw = honker_ops::claim_batch(&db.conn, "q", "w", 1, 30).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v[0]["id"], id, "claimed, not expired: {raw}");
    assert_eq!(v[0]["claimed_at"], start);
    assert_eq!(v[0]["claim_expires_at"], start + 30);
    assert_eq!(db.now(), start + 1, "exactly one clock reading per claim");
}

/// The dead-letter moves read no clock of their own either: the rows a
/// claim expires carry the claim's reading as `died_at`.
#[test]
fn a_claim_that_dead_letters_still_reads_the_clock_once() {
    let db = Db::with_clock(true);
    let expired = db.enqueue(None, 0, None);
    let live = db.enqueue(None, 0, None);
    let start = db.now();
    db.conn
        .execute(
            "UPDATE _honker_live SET expires_at = ?1 WHERE id = ?2",
            [start - 1, expired],
        )
        .unwrap();
    assert_eq!(db.claim("w", 30), Some((live, 1)));
    assert_eq!(db.dead_error(expired).as_deref(), Some("expired"));
    let died_at: i64 = db
        .conn
        .query_row(
            "SELECT died_at FROM _honker_dead WHERE id = ?1",
            [expired],
            |r| r.get(0),
        )
        .unwrap();
    assert_eq!(died_at, start);
    assert_eq!(db.now(), start + 1, "exactly one clock reading per claim");
}

// ------------------------------------------------------------ query plans

fn plan(conn: &Connection, sql: &str, params: impl rusqlite::Params) -> String {
    let mut stmt = conn.prepare(&format!("EXPLAIN QUERY PLAN {sql}")).unwrap();
    stmt.query_map(params, |r| r.get::<_, String>(3))
        .unwrap()
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
        .join(" | ")
}

/// Each step reads the index that holds only rows it can act on. A
/// plan that falls back to a scan of the queue makes the claim O(rows).
#[test]
fn claim_steps_use_their_indexes() {
    let db = Db::new();
    let p = plan(
        &db.conn,
        "UPDATE _honker_live SET state = 'pending' WHERE id IN (
           SELECT id FROM _honker_live
           WHERE queue = ?1 AND state = 'scheduled' AND run_at <= ?2
           ORDER BY run_at, id LIMIT ?3)",
        rusqlite::params!["q", 1, 1],
    );
    assert!(p.contains("_honker_live_scheduled"), "promote: {p}");
    let p = plan(
        &db.conn,
        "SELECT id FROM _honker_live
         WHERE queue = ?1 AND expires_at <= ?2
           AND (state IN ('pending', 'scheduled')
                OR (state = 'processing' AND claim_expires_at < ?2))
         ORDER BY expires_at, id LIMIT ?3",
        rusqlite::params!["q", 1, 1],
    );
    assert!(p.contains("_honker_live_expiry"), "expire: {p}");
    let p = plan(
        &db.conn,
        "SELECT id FROM _honker_live
         WHERE queue = ?1 AND state = 'processing' AND claim_expires_at < ?2
           AND attempts >= max_attempts
         ORDER BY claim_expires_at, id LIMIT ?3",
        rusqlite::params!["q", 1, 1],
    );
    assert!(
        p.contains("_honker_live_processing_deadline"),
        "exhausted: {p}"
    );
    let p = plan(
        &db.conn,
        "SELECT id FROM (
           SELECT id, priority, run_at FROM (
             SELECT id, priority, run_at
             FROM _honker_live INDEXED BY _honker_live_ready
             WHERE queue = ?1 AND state = 'pending' AND run_at <= ?2
               AND attempts < max_attempts
               AND (expires_at IS NULL OR expires_at > ?2)
             ORDER BY priority DESC, run_at, id LIMIT ?3)
           UNION ALL
           SELECT id, priority, run_at FROM _honker_live
           WHERE queue = ?1 AND state = 'processing' AND claim_expires_at < ?2
             AND attempts < max_attempts
             AND (expires_at IS NULL OR expires_at > ?2))
         ORDER BY priority DESC, run_at, id LIMIT ?3",
        rusqlite::params!["q", 1, 1],
    );
    assert!(p.contains("_honker_live_ready"), "claim ready arm: {p}");
    assert!(
        p.contains("_honker_live_processing_deadline"),
        "claim lapsed arm: {p}"
    );
    // The ready arm walks the index in claim order: the only sort is the
    // outer merge of the two arms' (at most n + lapsed) rows.
    assert_eq!(p.matches("TEMP B-TREE").count(), 1, "claim: {p}");
}

// ------------------------------------------------------------ migration

/// The `_honker_live` schema and indexes of builds before `scheduled`.
const LEGACY_LIVE: &str = "
    CREATE TABLE _honker_live (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      queue TEXT NOT NULL,
      payload TEXT NOT NULL,
      state TEXT NOT NULL DEFAULT 'pending',
      priority INTEGER NOT NULL DEFAULT 0,
      run_at INTEGER NOT NULL DEFAULT (unixepoch()),
      worker_id TEXT,
      claim_expires_at INTEGER,
      attempts INTEGER NOT NULL DEFAULT 0,
      max_attempts INTEGER NOT NULL DEFAULT 3,
      created_at INTEGER NOT NULL DEFAULT (unixepoch()),
      expires_at INTEGER,
      claimed_at INTEGER
    );
    CREATE INDEX _honker_live_claim
      ON _honker_live(queue, priority DESC, run_at, id)
      WHERE state IN ('pending', 'processing');
    CREATE INDEX _honker_live_pending_deadline
      ON _honker_live(queue, run_at)
      WHERE state = 'pending';
    CREATE INDEX _honker_live_processing_deadline
      ON _honker_live(queue, claim_expires_at)
      WHERE state = 'processing';
";

fn indexes(conn: &Connection) -> Vec<String> {
    conn.prepare(
        "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = '_honker_live'
         AND name LIKE '_honker_live_%' ORDER BY name",
    )
    .unwrap()
    .query_map([], |r| r.get(0))
    .unwrap()
    .collect::<Result<Vec<_>, _>>()
    .unwrap()
}

#[test]
fn bootstrap_migrates_a_legacy_database_once() {
    let conn = Connection::open_in_memory().unwrap();
    conn.execute_batch(LEGACY_LIVE).unwrap();
    let now: i64 = conn
        .query_row("SELECT unixepoch()", [], |r| r.get(0))
        .unwrap();
    let rows = [
        // (state, run_at offset, attempts, max_attempts, claim_expires_at offset)
        ("pending", 3600, 0, 3, None),        // 1 future -> scheduled
        ("pending", -10, 0, 3, None),         // 2 due -> pending
        ("pending", -10, 3, 3, None),         // 3 exhausted due -> dead
        ("pending", 3600, 2, 2, None),        // 4 exhausted future -> dead
        ("processing", -10, 1, 3, Some(600)), // 5 in flight -> untouched
        ("processing", -10, 1, 1, Some(-5)),  // 6 lapsed + exhausted -> left for claim
    ];
    for (state, run, att, max, cexp) in rows {
        conn.execute(
            "INSERT INTO _honker_live
               (queue, payload, state, run_at, attempts, max_attempts,
                worker_id, claim_expires_at)
             VALUES ('q', '{}', ?1, ?2, ?3, ?4, ?5, ?6)",
            rusqlite::params![
                state,
                now + run,
                att,
                max,
                cexp.map(|_| "old"),
                cexp.map(|c: i64| now + c)
            ],
        )
        .unwrap();
    }

    bootstrap_honker_schema(&conn).unwrap();
    attach_honker_functions(&conn).unwrap();

    let state = |id: i64| -> Option<String> {
        conn.query_row("SELECT state FROM _honker_live WHERE id = ?1", [id], |r| {
            r.get(0)
        })
        .optional()
        .unwrap()
    };
    assert_eq!(state(1).as_deref(), Some("scheduled"));
    assert_eq!(state(2).as_deref(), Some("pending"));
    assert_eq!(state(3), None);
    assert_eq!(state(4), None);
    assert_eq!(state(5).as_deref(), Some("processing"));
    assert_eq!(state(6).as_deref(), Some("processing"));
    let dead: Vec<(i64, String)> = conn
        .prepare("SELECT id, last_error FROM _honker_dead ORDER BY id")
        .unwrap()
        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
        .unwrap()
        .collect::<Result<_, _>>()
        .unwrap();
    assert_eq!(
        dead,
        vec![
            (3, "max attempts exceeded".to_string()),
            (4, "max attempts exceeded".to_string())
        ]
    );
    assert_eq!(
        indexes(&conn),
        vec![
            "_honker_live_expiry",
            "_honker_live_processing_deadline",
            "_honker_live_ready",
            "_honker_live_scheduled",
        ]
    );

    // A second bootstrap changes nothing.
    let snapshot = |c: &Connection| -> String {
        c.query_row(
            "SELECT json_group_array(json_array(id, state, attempts)) FROM _honker_live",
            [],
            |r| r.get(0),
        )
        .unwrap()
    };
    let before = snapshot(&conn);
    bootstrap_honker_schema(&conn).unwrap();
    assert_eq!(snapshot(&conn), before);

    // Claims behave: the lapsed exhausted row dies, the due row is claimed.
    let raw = honker_ops::claim_batch(&conn, "q", "new", 5, 60).unwrap();
    let v: serde_json::Value = serde_json::from_str(&raw).unwrap();
    assert_eq!(v.as_array().unwrap().len(), 1, "{raw}");
    assert_eq!(v[0]["id"], 2);
    assert_eq!(state(6), None);
}

/// A legacy `pending` row with a future `run_at` that slipped in after
/// the migration (an old worker that was not stopped) is still not run
/// early: the claim's `run_at <= now` filter holds it back.
#[test]
fn a_future_pending_row_from_an_old_writer_is_not_claimed_early() {
    let db = Db::new();
    let now = db.now();
    db.conn
        .execute(
            "INSERT INTO _honker_live (queue, payload, state, run_at) VALUES ('q', '{}', 'pending', ?1)",
            [now + 10],
        )
        .unwrap();
    assert_eq!(db.claim("w", 30), None);
    db.advance(10);
    assert!(db.claim("w", 30).is_some());
}
