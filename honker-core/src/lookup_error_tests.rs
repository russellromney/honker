use crate::{attach_honker_functions, bootstrap_honker_schema};
use rusqlite::Connection;

fn db() -> Connection {
    let conn = Connection::open_in_memory().unwrap();
    attach_honker_functions(&conn).unwrap();
    bootstrap_honker_schema(&conn).unwrap();
    conn
}

fn scalar(conn: &Connection, sql: &str) -> rusqlite::Result<i64> {
    conn.query_row(sql, [], |r| r.get(0))
}

fn missing_table_is_error(table: &str, sql: &str) {
    let conn = db();
    conn.execute_batch(&format!("DROP TABLE {table}")).unwrap();
    let err = scalar(&conn, sql).expect_err("database damage must not look like absence");
    assert!(err.to_string().contains("no such table"), "{err}");
}

#[test]
fn queue_deadline_missing_table_is_error() {
    missing_table_is_error("_honker_live", "SELECT honker_queue_next_claim_at('q')");
}

#[test]
fn scheduler_deadline_missing_table_is_error() {
    missing_table_is_error(
        "_honker_scheduler_tasks",
        "SELECT honker_scheduler_soonest()",
    );
}

#[test]
fn scheduler_update_missing_table_is_error() {
    missing_table_is_error(
        "_honker_scheduler_tasks",
        "SELECT honker_scheduler_update('task',NULL,NULL,7,NULL,0)",
    );
}

#[test]
fn checkpoint_missing_table_is_error() {
    missing_table_is_error(
        "_honker_stream_consumers",
        "SELECT honker_stream_get_offset('c','t')",
    );
}

#[test]
fn genuinely_absent_values_keep_their_existing_zero_results() {
    let conn = db();
    for sql in [
        "SELECT honker_queue_next_claim_at('q')",
        "SELECT honker_scheduler_soonest()",
        "SELECT honker_scheduler_update('missing',NULL,NULL,7,NULL,0)",
        "SELECT honker_stream_get_offset('c','t')",
    ] {
        assert_eq!(scalar(&conn, sql).unwrap(), 0, "{sql}");
    }
    assert!(conn.is_autocommit());
}

#[test]
fn queue_deadline_bad_type_is_error_and_valid_deadline_is_exact() {
    let conn = db();
    scalar(
        &conn,
        "SELECT honker_enqueue('q','{}',4000000000,NULL,0,3,NULL)",
    )
    .unwrap();
    assert_eq!(
        scalar(&conn, "SELECT honker_queue_next_claim_at('q')").unwrap(),
        4_000_000_000
    );
    conn.execute_batch("UPDATE _honker_live SET run_at='bad timestamp'")
        .unwrap();
    let err = scalar(&conn, "SELECT honker_queue_next_claim_at('q')").unwrap_err();
    assert!(err.to_string().contains("Invalid column type"), "{err}");
}

#[test]
fn scheduler_deadline_bad_type_is_error_and_update_still_works() {
    let conn = db();
    scalar(
        &conn,
        "SELECT honker_scheduler_register('task','q','@every 1m','{}',0,NULL)",
    )
    .unwrap();
    assert_eq!(
        scalar(
            &conn,
            "SELECT honker_scheduler_update('task',NULL,NULL,7,NULL,0)"
        )
        .unwrap(),
        1
    );
    assert_eq!(
        scalar(
            &conn,
            "SELECT priority FROM _honker_scheduler_tasks WHERE name='task'"
        )
        .unwrap(),
        7
    );
    conn.execute_batch("UPDATE _honker_scheduler_tasks SET next_fire_at=4000000000")
        .unwrap();
    assert_eq!(
        scalar(&conn, "SELECT honker_scheduler_soonest()").unwrap(),
        4_000_000_000
    );
    conn.execute_batch("UPDATE _honker_scheduler_tasks SET next_fire_at='bad timestamp'")
        .unwrap();
    let err = scalar(&conn, "SELECT honker_scheduler_soonest()").unwrap_err();
    assert!(err.to_string().contains("Invalid column type"), "{err}");
}

#[test]
fn checkpoint_bad_type_is_error_not_a_restart_from_zero() {
    let conn = db();
    scalar(&conn, "SELECT honker_stream_save_offset('c','t',41)").unwrap();
    assert_eq!(
        scalar(&conn, "SELECT honker_stream_get_offset('c','t')").unwrap(),
        41
    );
    conn.execute_batch("UPDATE _honker_stream_consumers SET offset='broken'")
        .unwrap();
    let err = scalar(&conn, "SELECT honker_stream_get_offset('c','t')").unwrap_err();
    assert!(err.to_string().contains("Invalid column type"), "{err}");
    let stored: String = conn
        .query_row("SELECT offset FROM _honker_stream_consumers", [], |r| {
            r.get(0)
        })
        .unwrap();
    assert_eq!(
        stored, "broken",
        "reading must not repair or reset the checkpoint"
    );
}
