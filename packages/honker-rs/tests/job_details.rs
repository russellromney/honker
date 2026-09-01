//! Issue #136: claimed jobs and read-only snapshots must carry every
//! field `honker_claim_batch()` / `honker_get_job()` return, and the
//! typed `Queue<T>` must decode payloads into the caller's type.
//!
//! Every value asserted here is chosen to be distinguishable from the
//! others. Defaults (`priority` 0, `max_attempts` 3, a 300s
//! visibility timeout, `run_at` == `created_at`) would let a wrong
//! field pass for the right one, so the fixture uses odd constants
//! and back-dates `run_at` well away from `created_at`.

use honker::{Database, EnqueueOpts, QueueOpts};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::{SystemTime, UNIX_EPOCH};

fn open_db() -> (tempfile::TempDir, Database) {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    (tmp, db)
}

fn now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// Fixture constants. All distinct, none a default, none derivable
/// from another by accident.
const VISIBILITY_TIMEOUT_S: i64 = 137;
const MAX_ATTEMPTS: i64 = 9;
const HIGH_PRIORITY: i64 = 42;
const LOW_PRIORITY: i64 = 7;
const RUN_AT_BACKDATE_S: i64 = 300;
const EXPIRES_S: i64 = 9_000;

fn parity_queue(db: &Database) -> honker::Queue {
    db.queue(
        "details",
        QueueOpts {
            visibility_timeout_s: VISIBILITY_TIMEOUT_S,
            max_attempts: MAX_ATTEMPTS,
        },
    )
}

#[test]
fn claimed_job_carries_every_core_field() {
    let (_tmp, db) = open_db();
    let q = parity_queue(&db);

    let enqueued_at_lo = now();
    // Back-dated run_at: claimable immediately, but far enough from
    // created_at that swapping the two fields cannot pass.
    let high_run_at = enqueued_at_lo - RUN_AT_BACKDATE_S;
    let high_id = q
        .enqueue(
            &json!({"to": "alice@example.com", "v": 1}),
            EnqueueOpts {
                run_at: Some(high_run_at),
                priority: HIGH_PRIORITY,
                expires: Some(EXPIRES_S),
                ..Default::default()
            },
        )
        .unwrap();
    let low_run_at = enqueued_at_lo - (RUN_AT_BACKDATE_S / 2);
    let low_id = q
        .enqueue(
            &json!({"to": "bob@example.com", "v": 1}),
            EnqueueOpts {
                run_at: Some(low_run_at),
                priority: LOW_PRIORITY,
                // No expires: proves expires_at is read per-row and
                // not filled from a constant.
                ..Default::default()
            },
        )
        .unwrap();
    let enqueued_at_hi = now();

    let claim_lo = now();
    let jobs = q.claim_batch("worker-parity", 2).unwrap();
    let claim_hi = now();
    assert_eq!(jobs.len(), 2, "both jobs should claim");

    // priority DESC decides the order, so the high-priority row is first.
    let high = &jobs[0];
    let low = &jobs[1];

    assert_eq!(high.id, high_id, "id");
    assert_eq!(low.id, low_id, "id of the second row");
    assert_eq!(high.queue, "details", "queue");

    assert_eq!(
        String::from_utf8(high.payload.clone()).unwrap(),
        r#"{"to":"alice@example.com","v":1}"#,
        "payload stays the raw JSON text as enqueued"
    );
    let decoded: serde_json::Value = high.payload_as().unwrap();
    assert_eq!(decoded["to"], "alice@example.com", "decoded payload");

    assert_eq!(high.state, "processing", "state");

    assert_eq!(high.priority, HIGH_PRIORITY, "priority");
    assert_eq!(
        low.priority, LOW_PRIORITY,
        "priority is per-row, not a constant"
    );

    assert_eq!(high.run_at, high_run_at, "run_at");
    assert_eq!(low.run_at, low_run_at, "run_at is per-row");
    assert_ne!(
        high.run_at, high.created_at,
        "run_at must not be created_at"
    );

    assert_eq!(high.worker_id, "worker-parity", "worker_id");

    assert!(
        high.claim_expires_at >= claim_lo + VISIBILITY_TIMEOUT_S
            && high.claim_expires_at <= claim_hi + VISIBILITY_TIMEOUT_S,
        "claim_expires_at should be the claim instant + {VISIBILITY_TIMEOUT_S}s, \
         got {} against a claim window of [{claim_lo}, {claim_hi}]",
        high.claim_expires_at
    );

    assert_eq!(high.attempts, 1, "attempts after the first claim");
    assert_eq!(high.max_attempts, MAX_ATTEMPTS, "max_attempts");

    assert!(
        high.created_at >= enqueued_at_lo && high.created_at <= enqueued_at_hi,
        "created_at should sit inside the enqueue window \
         [{enqueued_at_lo}, {enqueued_at_hi}], got {}",
        high.created_at
    );

    let expires_at = high.expires_at.expect("expires_at set for the high row");
    assert!(
        expires_at >= enqueued_at_lo + EXPIRES_S && expires_at <= enqueued_at_hi + EXPIRES_S,
        "expires_at should be the enqueue instant + {EXPIRES_S}s, got {expires_at} \
         against an enqueue window of [{enqueued_at_lo}, {enqueued_at_hi}]"
    );
    assert_eq!(
        low.expires_at, None,
        "expires_at stays null when the enqueue set no expiry"
    );
}

#[test]
fn claimed_job_attempts_counts_up_across_retries() {
    let (_tmp, db) = open_db();
    let q = parity_queue(&db);
    q.enqueue(&json!({"n": 1}), EnqueueOpts::default()).unwrap();

    let first = q.claim_one("w1").unwrap().expect("first claim");
    assert_eq!(first.attempts, 1, "attempts on the first claim");
    assert!(first.retry(0, "boom").unwrap());

    let second = q.claim_one("w2").unwrap().expect("second claim");
    assert_eq!(
        second.attempts, 2,
        "attempts must advance with the retry, not stick at 1"
    );
    assert_eq!(second.worker_id, "w2", "worker_id follows the new holder");
    assert_eq!(second.id, first.id, "same row, re-claimed");
    assert_eq!(
        second.created_at, first.created_at,
        "created_at is stable across claims"
    );
}

#[test]
fn snapshot_shows_pending_then_processing_then_misses() {
    let (_tmp, db) = open_db();
    let q = parity_queue(&db);

    let enqueued_at_lo = now();
    let id = q
        .enqueue(
            &json!({"to": "carol@example.com"}),
            EnqueueOpts {
                priority: HIGH_PRIORITY,
                expires: Some(EXPIRES_S),
                ..Default::default()
            },
        )
        .unwrap();
    let enqueued_at_hi = now();

    let pending = q.get_job(id).unwrap().expect("pending snapshot");
    assert_eq!(pending.id, id, "id");
    assert_eq!(pending.queue, "details", "queue");
    assert_eq!(
        pending.payload, r#"{"to":"carol@example.com"}"#,
        "snapshot payload stays raw JSON text"
    );
    assert_eq!(pending.state, "pending", "state before any claim");
    assert_eq!(pending.priority, HIGH_PRIORITY, "priority");
    assert!(
        pending.run_at >= enqueued_at_lo && pending.run_at <= enqueued_at_hi,
        "an undelayed run_at is the enqueue instant, got {}",
        pending.run_at
    );
    assert_eq!(pending.worker_id, None, "no holder while pending");
    assert_eq!(
        pending.claim_expires_at, None,
        "no claim deadline while pending"
    );
    assert_eq!(pending.attempts, 0, "attempts before any claim");
    assert_eq!(pending.max_attempts, MAX_ATTEMPTS, "max_attempts");
    assert!(
        pending.created_at >= enqueued_at_lo && pending.created_at <= enqueued_at_hi,
        "created_at inside the enqueue window, got {}",
        pending.created_at
    );
    let pending_expires = pending.expires_at.expect("expires_at set");
    assert!(
        pending_expires >= enqueued_at_lo + EXPIRES_S
            && pending_expires <= enqueued_at_hi + EXPIRES_S,
        "expires_at is the enqueue instant + {EXPIRES_S}s, got {pending_expires}"
    );

    // A second reader sees the processing details of someone else's claim.
    let claim_lo = now();
    let job = q.claim_one("worker-reader").unwrap().expect("claim");
    let claim_hi = now();

    let reader = Database::open(_tmp.path().join("t.db")).unwrap();
    let processing = parity_queue(&reader)
        .get_job(id)
        .unwrap()
        .expect("processing snapshot");
    assert_eq!(processing.state, "processing", "state while claimed");
    assert_eq!(
        processing.worker_id.as_deref(),
        Some("worker-reader"),
        "worker_id names the holder"
    );
    assert_eq!(processing.attempts, 1, "attempts after the claim");
    let deadline = processing
        .claim_expires_at
        .expect("claim_expires_at set while processing");
    assert!(
        deadline >= claim_lo + VISIBILITY_TIMEOUT_S && deadline <= claim_hi + VISIBILITY_TIMEOUT_S,
        "claim_expires_at is the claim instant + {VISIBILITY_TIMEOUT_S}s, got {deadline}"
    );
    // Unchanged by the claim.
    assert_eq!(processing.priority, HIGH_PRIORITY, "priority survives");
    assert_eq!(
        processing.max_attempts, MAX_ATTEMPTS,
        "max_attempts survives"
    );
    assert_eq!(
        processing.created_at, pending.created_at,
        "created_at survives"
    );
    assert_eq!(
        processing.expires_at, pending.expires_at,
        "expires_at survives"
    );

    assert!(job.ack().unwrap());
    assert!(
        parity_queue(&reader).get_job(id).unwrap().is_none(),
        "the reader sees no job after ack"
    );
}

#[test]
fn delayed_job_snapshot_reports_the_future_run_at() {
    let (_tmp, db) = open_db();
    let q = parity_queue(&db);

    const DELAY_S: i64 = 600;
    let lo = now();
    let id = q
        .enqueue(
            &json!({"later": true}),
            EnqueueOpts {
                delay: Some(DELAY_S),
                ..Default::default()
            },
        )
        .unwrap();
    let hi = now();

    let row = q.get_job(id).unwrap().expect("snapshot");
    assert!(
        row.run_at >= lo + DELAY_S && row.run_at <= hi + DELAY_S,
        "delayed run_at should be the enqueue instant + {DELAY_S}s, got {} \
         against an enqueue window of [{lo}, {hi}]",
        row.run_at
    );
    assert_eq!(
        row.run_at - row.created_at,
        DELAY_S,
        "run_at must sit exactly {DELAY_S}s after created_at"
    );
    assert_eq!(row.state, "pending", "a delayed job stays pending");
    assert!(
        q.claim_one("w").unwrap().is_none(),
        "a delayed job is not claimable yet"
    );
}

// ---------------------------------------------------------------------
// Typed payloads
// ---------------------------------------------------------------------

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct Email {
    version: u32,
    to: String,
    subject: String,
}

#[test]
fn typed_queue_round_trips_the_payload_type() {
    let (_tmp, db) = open_db();
    let q = db.typed_queue::<Email>("typed", QueueOpts::default());

    let sent = Email {
        version: 2,
        to: "dave@example.com".into(),
        subject: "quarterly honk".into(),
    };
    let id = q.enqueue(&sent, EnqueueOpts::default()).unwrap();

    let snapshot = q.get_job(id).unwrap().expect("snapshot");
    assert_eq!(
        snapshot.payload_typed().unwrap(),
        sent,
        "the snapshot decodes into the queue's payload type"
    );

    let job = q.claim_one("w").unwrap().expect("claim");
    assert_eq!(job.id, id, "id");
    assert_eq!(
        job.payload_typed().unwrap(),
        sent,
        "the claimed job decodes into the queue's payload type"
    );
    // A typed queue still carries the full field set.
    assert_eq!(job.state, "processing", "state");
    assert_eq!(job.worker_id, "w", "worker_id");
    assert_eq!(job.max_attempts, 3, "max_attempts from QueueOpts::default");
    assert!(job.ack().unwrap());
}

#[test]
fn typed_queue_reads_a_payload_another_writer_produced() {
    // Versioned payload written through one type, read through another.
    // honker does not check payload shape, so this only works because
    // the two shapes agree on the wire.
    #[derive(Serialize)]
    struct EmailV2Writer {
        version: u32,
        to: String,
        subject: String,
    }

    let (_tmp, db) = open_db();
    let writer = db.typed_queue::<EmailV2Writer>("versioned", QueueOpts::default());
    writer
        .enqueue(
            &EmailV2Writer {
                version: 2,
                to: "erin@example.com".into(),
                subject: "cross-type read".into(),
            },
            EnqueueOpts::default(),
        )
        .unwrap();

    let reader = writer.typed::<Email>();
    let job = reader.claim_one("w").unwrap().expect("claim");
    assert_eq!(
        job.payload_typed().unwrap(),
        Email {
            version: 2,
            to: "erin@example.com".into(),
            subject: "cross-type read".into(),
        },
        "the reader decodes the writer's payload"
    );
}

#[test]
fn payload_shape_is_never_validated_by_honker() {
    // The type parameter is a compile-time convenience only. honker
    // stores whatever JSON you hand it; the mismatch only surfaces
    // when serde tries to decode.
    let (_tmp, db) = open_db();
    let untyped = db.queue("mismatch", QueueOpts::default());
    let id = untyped
        .enqueue(&json!({"not": "an email"}), EnqueueOpts::default())
        .unwrap();

    let typed = untyped.typed::<Email>();
    let row = typed
        .get_job(id)
        .unwrap()
        .expect("the row is stored regardless of shape");
    assert_eq!(
        row.payload, r#"{"not":"an email"}"#,
        "honker stored the mismatched payload without complaint"
    );
    assert!(
        row.payload_typed().is_err(),
        "the mismatch surfaces as a serde error, not a honker error"
    );
}
