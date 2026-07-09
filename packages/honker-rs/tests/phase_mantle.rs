//! Phase Mantle tests: schedule lifecycle (pause/resume/list/update)
//! and queue cancel/get_job.

use honker::{Database, EnqueueOpts, QueueOpts, ScheduleUpdate, ScheduledTask};
use serde_json::json;

fn open_db() -> (tempfile::TempDir, Database) {
    let tmp = tempfile::tempdir().unwrap();
    let db = Database::open(tmp.path().join("t.db")).unwrap();
    (tmp, db)
}

#[test]
fn schedule_list_round_trips_all_fields() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();

    sched
        .add(ScheduledTask {
            name: "daily-recap".into(),
            queue: "emails".into(),
            schedule: "0 9 * * 1".into(),
            payload: json!({"team": "premier-league"}),
            priority: 3,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();
    sched
        .add(ScheduledTask {
            name: "hourly-sync".into(),
            queue: "syncs".into(),
            schedule: "@every 1h".into(),
            payload: json!(null),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();

    let rows = sched.list().unwrap();
    assert_eq!(rows.len(), 2);
    let recap = rows.iter().find(|r| r.name == "daily-recap").unwrap();
    assert_eq!(recap.queue, "emails");
    assert_eq!(recap.priority, 3);
    assert!(recap.enabled);
    assert!(recap.next_fire_at > 0);
    let payload: serde_json::Value = serde_json::from_str(&recap.payload).unwrap();
    assert_eq!(payload["team"], "premier-league");
}

#[test]
fn pause_resume_idempotent() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();
    sched
        .add(ScheduledTask {
            name: "a".into(),
            queue: "q".into(),
            schedule: "0 9 * * *".into(),
            payload: json!(null),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();

    assert!(sched.pause("a").unwrap());
    assert!(!sched.pause("a").unwrap()); // idempotent
    assert!(!sched.pause("missing").unwrap());

    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "a")
        .unwrap();
    assert!(!row.enabled);

    assert!(sched.resume("a").unwrap());
    assert!(!sched.resume("a").unwrap()); // already enabled

    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "a")
        .unwrap();
    assert!(row.enabled);
}

#[test]
fn update_mutates_fields_and_recomputes_next_fire_at() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();
    sched
        .add(ScheduledTask {
            name: "t".into(),
            queue: "q".into(),
            schedule: "0 9 * * *".into(),
            payload: json!({"v": 1}),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();

    assert!(
        sched
            .update(
                "t",
                ScheduleUpdate {
                    payload: Some(json!({"v": 99})),
                    priority: Some(5),
                    ..Default::default()
                },
            )
            .unwrap()
    );
    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "t")
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
    assert_eq!(payload["v"], 99);
    assert_eq!(row.priority, 5);

    let before = row.next_fire_at;
    assert!(
        sched
            .update(
                "t",
                ScheduleUpdate {
                    cron_expr: Some("*/5 * * * *".into()),
                    ..Default::default()
                },
            )
            .unwrap()
    );
    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "t")
        .unwrap();
    assert_eq!(row.cron_expr, "*/5 * * * *");
    assert_ne!(row.next_fire_at, before);

    assert!(
        !sched
            .update(
                "missing",
                ScheduleUpdate {
                    payload: Some(json!({})),
                    ..Default::default()
                },
            )
            .unwrap()
    );
}

#[test]
fn update_no_fields_is_noop() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();
    sched
        .add(ScheduledTask {
            name: "t".into(),
            queue: "q".into(),
            schedule: "0 9 * * *".into(),
            payload: json!({"v": 1}),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();

    let before: Vec<_> = sched
        .list()
        .unwrap()
        .into_iter()
        .map(|r| r.next_fire_at)
        .collect();
    assert!(!sched.update("t", ScheduleUpdate::default()).unwrap());
    let after: Vec<_> = sched
        .list()
        .unwrap()
        .into_iter()
        .map(|r| r.next_fire_at)
        .collect();
    assert_eq!(before, after);
}

#[test]
fn queue_get_job_returns_row_misses_after_cancel() {
    let (_tmp, db) = open_db();
    let q = db.queue("emails", QueueOpts::default());

    let id = q
        .enqueue(&json!({"to": "alice@example.com"}), EnqueueOpts::default())
        .unwrap();

    let row = q.get_job(id).unwrap().expect("should hit");
    assert_eq!(row.id, id);
    assert_eq!(row.queue, "emails");
    assert_eq!(row.state, "pending");

    assert!(q.cancel(id).unwrap());
    assert!(!q.cancel(id).unwrap()); // idempotent
    assert!(q.get_job(id).unwrap().is_none());
    assert!(q.claim_one("worker-1").unwrap().is_none());
}

#[test]
fn cancel_processing_invalidates_ack() {
    let (_tmp, db) = open_db();
    let q = db.queue("emails", QueueOpts::default());
    let id = q
        .enqueue(&json!({"to": "x"}), EnqueueOpts::default())
        .unwrap();
    let job = q.claim_one("worker-1").unwrap().unwrap();
    assert_eq!(job.id, id);

    assert!(q.cancel(id).unwrap());
    // Worker's ack now returns false (same shape as expired claim).
    assert!(!job.ack().unwrap());
}

#[test]
fn paused_schedule_does_not_emit_on_tick() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();
    sched
        .add(ScheduledTask {
            name: "due".into(),
            queue: "emails".into(),
            schedule: "@every 1s".into(),
            payload: json!({"x": 1}),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();
    std::thread::sleep(std::time::Duration::from_millis(1100));
    sched.pause("due").unwrap();

    let fires = sched.tick().unwrap();
    assert_eq!(
        fires.len(),
        0,
        "paused schedule must not emit; got {fires:?}"
    );

    // Resume and tick again — now it fires.
    sched.resume("due").unwrap();
    let fires2 = sched.tick().unwrap();
    assert!(
        !fires2.is_empty(),
        "resumed schedule should emit; got {fires2:?}"
    );
}

#[test]
fn queue_get_job_misses_after_ack() {
    let (_tmp, db) = open_db();
    let q = db.queue("emails", QueueOpts::default());
    let id = q
        .enqueue(&json!({"to": "x"}), EnqueueOpts::default())
        .unwrap();

    let job = q.claim_one("worker-1").unwrap().unwrap();
    assert_eq!(job.id, id);
    assert!(job.ack().unwrap());
    // Row gone after ack — get_job misses just like after cancel.
    assert!(q.get_job(id).unwrap().is_none());
}

#[test]
fn update_payload_null_vs_omitted() {
    let (_tmp, db) = open_db();
    let sched = db.scheduler();
    sched
        .add(ScheduledTask {
            name: "t".into(),
            queue: "q".into(),
            schedule: "0 9 * * *".into(),
            payload: json!({"v": 1}),
            priority: 0,
            expires_s: None,
            max_attempts: None,
        })
        .unwrap();

    // Omitted payload — payload field stays as Default (None).
    sched
        .update(
            "t",
            ScheduleUpdate {
                priority: Some(7),
                ..Default::default()
            },
        )
        .unwrap();
    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "t")
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
    assert_eq!(payload, json!({"v": 1}));
    assert_eq!(row.priority, 7);

    // payload: Some(Value::Null) — explicitly write JSON null.
    sched
        .update(
            "t",
            ScheduleUpdate {
                payload: Some(serde_json::Value::Null),
                ..Default::default()
            },
        )
        .unwrap();
    let row = sched
        .list()
        .unwrap()
        .into_iter()
        .find(|r| r.name == "t")
        .unwrap();
    let payload: serde_json::Value = serde_json::from_str(&row.payload).unwrap();
    assert!(payload.is_null());
}
