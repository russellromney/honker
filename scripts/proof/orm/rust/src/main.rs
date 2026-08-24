//! sqlx, as guides/orm/rust.mdx shows it.
// docs:start example

include!("../catalog.rs");

use sqlx::sqlite::{SqliteConnectOptions, SqliteRow};
use sqlx::{ConnectOptions, Connection, Executor, Row};
use std::str::FromStr;

fn cell(row: &SqliteRow) -> Value {
    if let Ok(v) = row.try_get::<Option<i64>, _>(0) {
        return v.map_or(Value::Null, |n| json!(n));
    }
    if let Ok(v) = row.try_get::<Option<f64>, _>(0) {
        return v.map_or(Value::Null, |n| json!(n as i64));
    }
    if let Ok(v) = row.try_get::<Option<String>, _>(0) {
        return v.map_or(Value::Null, Value::String);
    }
    Value::Null
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::var("HONKER_TEST_DB")?;
    let ext = std::env::var("HONKER_EXTENSION_PATH")?;

    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{db}"))?
        .create_if_missing(true)
        .extension(ext);
    let mut conn = opts.connect().await?;
    conn.execute("SELECT honker_bootstrap()").await?;

    let catalog: Value = serde_json::from_str(CATALOG)?;
    let mut vars: HashMap<String, Value> = HashMap::new();
    for step in catalog["steps"].as_array().unwrap() {
        let sql = step["sql"].as_str().unwrap();
        let mut query = sqlx::query(sql);
        for arg in step["args"].as_array().unwrap() {
            let resolved = resolve(arg, "rs", &vars)?;
            query = match resolved {
                Value::Null => query.bind(None::<String>),
                Value::Number(n) => query.bind(n.as_i64().unwrap()),
                Value::String(s) => query.bind(s),
                other => return Err(format!("cannot bind {other}").into()),
            };
        }
        let id = step["id"].as_str().unwrap();
        let row = query
            .fetch_one(&mut conn)
            .await
            .map_err(|e| format!("{id} failed: {e}"))?;
        let result = cell(&row);
        if let Some(store) = step["store"].as_str() {
            vars.insert(store.to_string(), result.clone());
        }
        if !step["expect"].is_null() {
            check(&step["expect"], &result, "rs", &vars).map_err(|e| format!("{id}: {e}"))?;
        }
    }

    conn.execute("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .await?;
    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO orders (id, user_id) VALUES (?, ?)")
        .bind(42_i64)
        .bind(1_i64)
        .execute(&mut *tx)
        .await?;
    let committed: i64 = sqlx::query("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")
        .bind("rs_atomic")
        .bind(r#"{"order_id":42}"#)
        .bind(0_i64)
        .bind(3_i64)
        .fetch_one(&mut *tx)
        .await?
        .get(0);
    tx.commit().await?;

    let n: i64 = sqlx::query("SELECT COUNT(*) FROM orders WHERE id = 42")
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert_eq!(n, 1);
    let job: String = sqlx::query("SELECT honker_get_job(?)")
        .bind(committed)
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert!(job.contains("order_id"), "{job}");

    let mut tx = conn.begin().await?;
    sqlx::query("INSERT INTO orders (id, user_id) VALUES (?, ?)")
        .bind(43_i64)
        .bind(1_i64)
        .execute(&mut *tx)
        .await?;
    let rolled: i64 = sqlx::query("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)")
        .bind("rs_atomic")
        .bind(r#"{"order_id":43}"#)
        .bind(0_i64)
        .bind(3_i64)
        .fetch_one(&mut *tx)
        .await?
        .get(0);
    tx.rollback().await?;

    let n: i64 = sqlx::query("SELECT COUNT(*) FROM orders WHERE id = 43")
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert_eq!(n, 0, "rollback left an order");
    let job: String = sqlx::query("SELECT honker_get_job(?)")
        .bind(rolled)
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert_eq!(job, "", "rollback left a job");

    println!("PASS rust-sqlx");
    Ok(())
}
// docs:end example
