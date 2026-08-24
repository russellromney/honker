//! Diesel, as guides/orm/rust.mdx shows it.

include!("../../catalog.rs");

use diesel::prelude::*;
use diesel::sql_query;
use diesel::sql_types::{BigInt, Integer, Nullable, Text};
use diesel::sqlite::{Sqlite, SqliteConnection};
use libloading::Library;
use std::os::raw::{c_char, c_int};

#[derive(QueryableByName)]
struct IdRow {
    #[diesel(sql_type = BigInt)]
    id: i64,
}

#[derive(QueryableByName)]
struct CountRow {
    #[diesel(sql_type = BigInt)]
    n: i64,
}

#[derive(QueryableByName)]
struct JobRow {
    #[diesel(sql_type = Text)]
    job: String,
}

type ExtensionInit = unsafe extern "C" fn(
    *mut libsqlite3_sys::sqlite3,
    *mut *mut c_char,
    *const libsqlite3_sys::sqlite3_api_routines,
) -> c_int;

fn open_honker(
    database_url: &str,
    extension_path: &str,
) -> Result<(SqliteConnection, Library), Box<dyn std::error::Error>> {
    let library = unsafe { Library::new(extension_path)? };
    let init: ExtensionInit = unsafe { *library.get(b"sqlite3_honkerext_init")? };
    let rc = unsafe { libsqlite3_sys::sqlite3_auto_extension(Some(init)) };
    if rc != libsqlite3_sys::SQLITE_OK {
        return Err(format!("sqlite3_auto_extension failed: {rc}").into());
    }

    let connection = SqliteConnection::establish(database_url);
    unsafe { libsqlite3_sys::sqlite3_cancel_auto_extension(Some(init)) };
    Ok((connection?, library))
}

#[derive(QueryableByName)]
struct ScalarRow {
    #[diesel(sql_type = Text)]
    value: String,
}

fn diesel_scalar(
    conn: &mut SqliteConnection,
    sql: &str,
    args: Vec<Value>,
) -> Result<Value, String> {
    let wrapped = format!(
        "WITH scalar(value) AS ({sql}) \
         SELECT json_object('type', typeof(value), 'value', value) AS value FROM scalar"
    );
    let mut query = sql_query(wrapped).into_boxed::<Sqlite>();
    for arg in args {
        query = match arg {
            Value::Null => query.bind::<Nullable<Text>, _>(None::<String>),
            Value::Number(n) => {
                query.bind::<BigInt, _>(n.as_i64().ok_or_else(|| format!("bad number {n}"))?)
            }
            Value::String(s) => query.bind::<Text, _>(s),
            other => return Err(format!("cannot bind {other}")),
        };
    }
    let row: ScalarRow = query.get_result(conn).map_err(|e| e.to_string())?;
    let envelope: Value = serde_json::from_str(&row.value).map_err(|e| e.to_string())?;
    match envelope["type"].as_str() {
        Some("null") => Ok(Value::Null),
        Some("integer") => Ok(json!(envelope["value"].as_i64().unwrap())),
        Some("real") => Ok(json!(envelope["value"].as_f64().unwrap() as i64)),
        _ => Ok(envelope["value"].clone()),
    }
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::var("HONKER_TEST_DB")?;
    let ext = std::env::var("HONKER_EXTENSION_PATH")?;

    let (mut conn, _honker_library) = open_honker(&db, &ext)?;
    sql_query("SELECT honker_bootstrap()").execute(&mut conn)?;

    run_catalog("di", |sql, args| diesel_scalar(&mut conn, sql, args))?;

    sql_query("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .execute(&mut conn)?;

    let committed = conn.transaction(|conn| {
        sql_query("INSERT INTO orders (id, user_id) VALUES (?, ?)")
            .bind::<Integer, _>(42)
            .bind::<Integer, _>(1)
            .execute(conn)?;
        sql_query("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id")
            .bind::<Text, _>("di_atomic")
            .bind::<Text, _>(r#"{"order_id":42}"#)
            .bind::<BigInt, _>(0_i64)
            .bind::<BigInt, _>(3_i64)
            .get_result::<IdRow>(conn)
            .map(|row| row.id)
    })?;

    let n: CountRow =
        sql_query("SELECT COUNT(*) AS n FROM orders WHERE id = 42").get_result(&mut conn)?;
    assert_eq!(n.n, 1, "missing committed order");
    let job: JobRow = sql_query("SELECT honker_get_job(?) AS job")
        .bind::<BigInt, _>(committed)
        .get_result(&mut conn)?;
    assert!(job.job.contains("order_id"), "{}", job.job);

    let mut rolled = None;
    let tx_res: Result<(), diesel::result::Error> = conn.transaction(|conn| {
        sql_query("INSERT INTO orders (id, user_id) VALUES (?, ?)")
            .bind::<Integer, _>(43)
            .bind::<Integer, _>(1)
            .execute(conn)?;
        rolled = Some(
            sql_query("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id")
                .bind::<Text, _>("di_atomic")
                .bind::<Text, _>(r#"{"order_id":43}"#)
                .bind::<BigInt, _>(0_i64)
                .bind::<BigInt, _>(3_i64)
                .get_result::<IdRow>(conn)?
                .id,
        );
        Err(diesel::result::Error::RollbackTransaction)
    });
    assert!(
        matches!(tx_res, Err(diesel::result::Error::RollbackTransaction)),
        "expected rollback, got {tx_res:?}"
    );
    let rolled = rolled.expect("enqueue before rollback");

    let n: CountRow =
        sql_query("SELECT COUNT(*) AS n FROM orders WHERE id = 43").get_result(&mut conn)?;
    assert_eq!(n.n, 0, "rollback left an order");
    let job: JobRow = sql_query("SELECT honker_get_job(?) AS job")
        .bind::<BigInt, _>(rolled)
        .get_result(&mut conn)?;
    assert_eq!(job.job, "", "rollback left a job");

    println!("PASS rust-diesel");
    Ok(())
}
