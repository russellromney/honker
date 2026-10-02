use crate::{attach_honker_functions, bootstrap_honker_schema, honker_ops};
use rusqlite::{Connection, Error, types::Value};

/// Operations that open a savepoint, so SQLite refuses them while a write
/// statement is active. A retry that only puts the job back to pending is
/// one UPDATE with no savepoint and is not restricted; see
/// `pending_retry_works_in_write_contexts`.
const RESTRICTED: [&str; 4] = ["claim", "fail", "sweep", "retry_dead"];
const ALL: [&str; 5] = ["claim", "fail", "sweep", "retry_pending", "retry_dead"];

/// The public SQL function each operation calls.
fn public_name(op: &str) -> &'static str {
    match op {
        "claim" => "honker_claim_batch",
        "fail" => "honker_fail",
        "sweep" => "honker_sweep_expired",
        _ => "honker_retry",
    }
}

fn setup(op: &str) -> (Connection, String) {
    let c = Connection::open_in_memory().unwrap();
    attach_honker_functions(&c).unwrap();
    bootstrap_honker_schema(&c).unwrap();
    c.execute_batch("CREATE TABLE app(x)").unwrap();
    let max = if op == "retry_dead" { 1 } else { 3 };
    let id = honker_ops::enqueue(&c, "q", "{}", None, None, 0, max, None).unwrap();
    if matches!(op, "fail" | "retry_pending" | "retry_dead") {
        honker_ops::claim_batch(&c, "q", "w", 1, 300).unwrap();
    }
    if op == "sweep" {
        c.execute_batch("UPDATE _honker_live SET expires_at=unixepoch()-10")
            .unwrap();
    }
    let call = match op {
        "claim" => "honker_claim_batch('q','w',1,300)".to_string(),
        "fail" => format!("honker_fail({id},'w','boom')"),
        "sweep" => "honker_sweep_expired('q')".to_string(),
        _ => format!("honker_retry({id},'w',0,'boom')"),
    };
    (c, call)
}

fn check_error(op: &str, err: Error) {
    let text = err.to_string();
    let name = public_name(op);
    assert!(
        text.contains(&format!("honker: {name} requires a separate SELECT")),
        "the error must name {name}: {text}"
    );
    assert!(
        text.contains("finish all write/RETURNING cursors"),
        "{text}"
    );
    assert!(
        text.contains("explicit surrounding transaction is supported"),
        "{text}"
    );
}

fn live(c: &Connection) -> String {
    honker_ops::get_job(c, 1).unwrap()
}
fn count(c: &Connection, table: &str) -> i64 {
    c.query_row(&format!("SELECT count(*) FROM {table}"), [], |r| r.get(0))
        .unwrap()
}

#[test]
fn write_statement_and_trigger_calls_fail_without_changing_jobs() {
    for op in RESTRICTED {
        for context in ["insert", "returning", "trigger"] {
            let (c, call) = setup(op);
            let before = live(&c);
            let sql = match context {
                "insert" => format!("INSERT INTO app SELECT {call}"),
                "returning" => format!("INSERT INTO app VALUES (1) RETURNING {call}"),
                _ => {
                    c.execute_batch(&format!(
                        "CREATE TRIGGER job_action AFTER INSERT ON app BEGIN SELECT {call}; END"
                    ))
                    .unwrap();
                    "INSERT INTO app VALUES (1)".to_string()
                }
            };
            let err = c
                .execute_batch(&sql)
                .expect_err("write contexts must fail explicitly");
            check_error(op, err);
            assert_eq!(live(&c), before, "{op} in {context}");
            assert_eq!(count(&c, "_honker_dead"), 0);
            assert_eq!(count(&c, "app"), 0, "outer write must fail too");
            assert!(c.is_autocommit());
        }
    }
}

#[test]
fn unfinished_returning_cursor_requires_finishing_before_separate_select() {
    for op in RESTRICTED {
        let (c, call) = setup(op);
        let before = live(&c);
        {
            let mut stmt = c
                .prepare("INSERT INTO app VALUES (1),(2) RETURNING x")
                .unwrap();
            let mut rows = stmt.query([]).unwrap();
            assert!(rows.next().unwrap().is_some());
            let err = c
                .query_row(&format!("SELECT {call}"), [], |r| r.get::<_, Value>(0))
                .unwrap_err();
            check_error(op, err);
            assert_eq!(live(&c), before);
            assert_eq!(count(&c, "_honker_dead"), 0);
            while rows.next().unwrap().is_some() {}
        }
        c.query_row(&format!("SELECT {call}"), [], |r| r.get::<_, Value>(0))
            .unwrap();
        assert_ne!(
            live(&c),
            before,
            "{op} must work after the cursor is finished"
        );
        assert_eq!(count(&c, "app"), 2);
    }
}

#[test]
fn separate_select_after_completed_write_keeps_callers_transaction() {
    for op in ALL {
        let (c, call) = setup(op);
        let before = live(&c);
        c.execute_batch("BEGIN; INSERT INTO app VALUES (42)")
            .unwrap();
        c.query_row(&format!("SELECT {call}"), [], |r| r.get::<_, Value>(0))
            .unwrap();
        assert!(!c.is_autocommit());
        assert_ne!(
            live(&c),
            before,
            "operation must have acted before rollback: {op}"
        );
        assert_eq!(count(&c, "app"), 1);
        c.execute_batch("ROLLBACK").unwrap();
        assert_eq!(live(&c), before, "caller rollback must undo {op}");
        assert_eq!(count(&c, "app"), 0);
        assert_eq!(count(&c, "_honker_dead"), 0);
    }
}

#[test]
fn pending_retry_works_in_write_contexts() {
    for context in ["insert", "returning", "trigger", "open_cursor"] {
        let (c, call) = setup("retry_pending");
        match context {
            "insert" => c
                .execute_batch(&format!("INSERT INTO app SELECT {call}"))
                .unwrap(),
            "returning" => c
                .execute_batch(&format!("INSERT INTO app VALUES (1) RETURNING {call}"))
                .unwrap(),
            "trigger" => c
                .execute_batch(&format!(
                    "CREATE TRIGGER job_action AFTER INSERT ON app BEGIN SELECT {call}; END;
                     INSERT INTO app VALUES (1)"
                ))
                .unwrap(),
            _ => {
                let mut stmt = c
                    .prepare("INSERT INTO app VALUES (1),(2) RETURNING x")
                    .unwrap();
                let mut rows = stmt.query([]).unwrap();
                assert!(rows.next().unwrap().is_some());
                let n: i64 = c
                    .query_row(&format!("SELECT {call}"), [], |r| r.get(0))
                    .unwrap();
                assert_eq!(n, 1);
                while rows.next().unwrap().is_some() {}
            }
        }
        let job: serde_json::Value = serde_json::from_str(&live(&c)).unwrap();
        assert_eq!(job["state"], "pending", "pending retry in {context}");
        assert!(
            count(&c, "app") >= 1,
            "outer write must commit in {context}"
        );
        assert!(c.is_autocommit());
    }
}
