//! SeaORM, as guides/orm/rust.mdx shows it.
// docs:start example

include!("../../catalog.rs");

use sea_orm::{
    ConnectionTrait, Database, DbBackend, DbErr, QueryResult, Statement, TransactionTrait,
    Value as SeaValue,
};
fn to_sea(value: &Value) -> SeaValue {
    match value {
        Value::Null => SeaValue::BigInt(None),
        Value::Number(n) => SeaValue::BigInt(n.as_i64()),
        Value::String(s) => SeaValue::String(Some(s.clone())),
        other => SeaValue::String(Some(other.to_string())),
    }
}

fn cell(row: &QueryResult) -> Value {
    if let Ok(v) = row.try_get_by_index::<Option<i64>>(0) {
        return match v {
            Some(n) => json!(n),
            None => Value::Null,
        };
    }
    if let Ok(v) = row.try_get_by_index::<Option<String>>(0) {
        return match v {
            Some(s) => Value::String(s),
            None => Value::Null,
        };
    }
    Value::Null
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_path = std::env::var("HONKER_TEST_DB")?;
    let ext = std::env::var("HONKER_EXTENSION_PATH")?;

    let mut opt = sea_orm::ConnectOptions::new(format!("sqlite://{db_path}?mode=rwc"));
    let ext_clone = ext.clone();
    opt.map_sqlx_sqlite_opts(move |opts| unsafe { opts.extension(ext_clone.clone()) });
    let db = Database::connect(opt).await?;
    db.execute_unprepared("SELECT honker_bootstrap()").await?;

    let catalog: Value = serde_json::from_str(CATALOG)?;
    let mut vars: HashMap<String, Value> = HashMap::new();
    for step in catalog["steps"].as_array().unwrap() {
        let sql = step["sql"].as_str().unwrap();
        let args: Vec<SeaValue> = step["args"]
            .as_array()
            .unwrap()
            .iter()
            .map(|arg| resolve(arg, "so", &vars).map(|v| to_sea(&v)))
            .collect::<Result<_, _>>()?;
        let id = step["id"].as_str().unwrap();
        let stmt = Statement::from_sql_and_values(DbBackend::Sqlite, sql, args);
        let row = db
            .query_one_raw(stmt)
            .await
            .map_err(|e| format!("{id} failed: {e}"))?
            .ok_or_else(|| format!("{id} returned no row"))?;
        let result = cell(&row);
        if let Some(store) = step["store"].as_str() {
            vars.insert(store.to_string(), result.clone());
        }
        if !step["expect"].is_null() {
            check(&step["expect"], &result, "so", &vars).map_err(|e| format!("{id}: {e}"))?;
        }
    }

    db.execute_unprepared("CREATE TABLE orders (id INTEGER PRIMARY KEY, user_id INTEGER)")
        .await?;

    let committed = db
        .transaction::<_, i64, DbErr>(|txn| {
            Box::pin(async move {
                txn.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO orders (id, user_id) VALUES (?, ?)",
                    [42i32.into(), 1i32.into()],
                ))
                .await?;
                let row = txn
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
                        [
                            "so_atomic".into(),
                            r#"{"order_id":42}"#.into(),
                            0i64.into(),
                            3i64.into(),
                        ],
                    ))
                    .await?
                    .ok_or_else(|| DbErr::Custom("missing enqueue row".into()))?;
                row.try_get_by_index(0)
            })
        })
        .await?;

    let n = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT COUNT(*) FROM orders WHERE id = 42",
            [],
        ))
        .await?
        .ok_or("missing count")?;
    assert_eq!(n.try_get_by_index::<i64>(0)?, 1, "missing committed order");
    let job = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT honker_get_job(?)",
            [committed.into()],
        ))
        .await?
        .ok_or("missing job")?;
    let job_s: String = job.try_get_by_index(0)?;
    assert!(job_s.contains("order_id"), "{job_s}");

    let rolled = std::sync::Arc::new(std::sync::Mutex::new(None::<i64>));
    let rolled_slot = rolled.clone();
    let tx_res = db
        .transaction::<_, (), DbErr>(|txn| {
            Box::pin(async move {
                txn.execute_raw(Statement::from_sql_and_values(
                    DbBackend::Sqlite,
                    "INSERT INTO orders (id, user_id) VALUES (?, ?)",
                    [43i32.into(), 1i32.into()],
                ))
                .await?;
                let row = txn
                    .query_one_raw(Statement::from_sql_and_values(
                        DbBackend::Sqlite,
                        "SELECT honker_enqueue(?, ?, NULL, NULL, ?, ?, NULL)",
                        [
                            "so_atomic".into(),
                            r#"{"order_id":43}"#.into(),
                            0i64.into(),
                            3i64.into(),
                        ],
                    ))
                    .await?
                    .ok_or_else(|| DbErr::Custom("missing enqueue row".into()))?;
                let id: i64 = row.try_get_by_index(0)?;
                *rolled_slot.lock().unwrap() = Some(id);
                Err(DbErr::Custom("rollback".into()))
            })
        })
        .await;
    assert!(tx_res.is_err(), "expected rollback, got {tx_res:?}");
    let rolled = rolled.lock().unwrap().expect("enqueue before rollback");

    let n = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT COUNT(*) FROM orders WHERE id = 43",
            [],
        ))
        .await?
        .ok_or("missing count")?;
    assert_eq!(n.try_get_by_index::<i64>(0)?, 0, "rollback left an order");
    let job = db
        .query_one_raw(Statement::from_sql_and_values(
            DbBackend::Sqlite,
            "SELECT honker_get_job(?)",
            [rolled.into()],
        ))
        .await?
        .ok_or("missing job")?;
    let job_s: String = job.try_get_by_index(0).unwrap_or_default();
    assert_eq!(job_s, "", "rollback left a job");

    println!("PASS rust-seaorm");
    Ok(())
}
// docs:end example
