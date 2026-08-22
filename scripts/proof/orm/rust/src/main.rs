//! sqlx, as guides/orm/rust.mdx shows it: SqliteConnectOptions::extension
//! with a path from HONKER_EXTENSION_PATH.

use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{ConnectOptions, Executor, Row};
use std::str::FromStr;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = std::env::var("HONKER_TEST_DB")?;
    let ext = std::env::var("HONKER_EXTENSION_PATH")?;

    let opts = SqliteConnectOptions::from_str(&format!("sqlite://{db}"))?
        .create_if_missing(true)
        .extension(ext);
    let mut conn = opts.connect().await?;
    conn.execute("SELECT honker_bootstrap()").await?;

    // Bound parameters, so sqlx's own binding is what is exercised.
    let payload = serde_json::json!({ "to": "alice@example.com" }).to_string();
    let id: i64 = sqlx::query("SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL) AS id")
        .bind("emails")
        .bind(&payload)
        .bind(0_i64)
        .bind(3_i64)
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert!(id > 0, "expected a job id, got {id}");

    let claimed_json: String = sqlx::query("SELECT honker_claim_batch(?, ?, ?, ?) AS jobs")
        .bind("emails")
        .bind("w1")
        .bind(8_i64)
        .bind(300_i64)
        .fetch_one(&mut conn)
        .await?
        .get(0);
    let claimed: serde_json::Value = serde_json::from_str(&claimed_json)?;
    assert_eq!(claimed.as_array().map(Vec::len), Some(1), "got {claimed_json}");
    assert_eq!(claimed[0]["id"].as_i64(), Some(id));

    let acked: i64 = sqlx::query("SELECT honker_ack(?, ?) AS ok")
        .bind(id)
        .bind("w1")
        .fetch_one(&mut conn)
        .await?
        .get(0);
    assert_eq!(acked, 1, "ack must match the claim");

    println!("PASS rust-sqlx");
    Ok(())
}
