//! Rust implementations of the `honker_*` SQL scalar functions, plus a
//! single `attach_honker_functions` helper that registers them on a
//! [`rusqlite::Connection`].
//!
//! Consumers:
//!   * `honker-extension` — the loadable SQLite extension. Calls
//!     `attach_honker_functions` so `.load ./libhonker_ext` in any
//!     SQLite client exposes the full function set.
//!   * `packages/honker` — the PyO3 binding. Calls
//!     `attach_honker_functions` on its writer connection so Python
//!     can invoke `SELECT honker_*(...)` inside its own transactions
//!     without loading the `.dylib` at runtime.
//!   * Future bindings (Go, Ruby, napi-rs) — load the extension via
//!     SQLite's `sqlite3_load_extension` and get the same functions
//!     for free.
//!
//! Rationale: each per-language binding would otherwise re-implement
//! this SQL. Moving it here gives us one source of truth that's
//! tested once and inherited by every consumer.

use rusqlite::Connection;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ValueRef;
use serde_json::{Value, json};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

const QUEUE_EVENTS_TOPIC: &str = "_honker:queue-events:v1";
const QUEUE_EVENT_CONFIG_CACHE_TTL: Duration = Duration::from_millis(100);

struct QueueEventRecord<'a> {
    event_type: &'a str,
    job_id: i64,
    queue: &'a str,
    payload: &'a str,
    attempts: i64,
    worker_id: Option<&'a str>,
    run_at: Option<i64>,
    reason: Option<&'a str>,
    error: Option<&'a str>,
}

#[derive(Clone, Copy)]
struct QueueEventConfig {
    retention_target: i64,
    include_payload: bool,
}

struct QueueEventConfigCache {
    encoded: AtomicU64,
    expires_at_ms: AtomicU64,
    bypass_until_autocommit: AtomicBool,
}

impl Default for QueueEventConfigCache {
    fn default() -> Self {
        Self {
            encoded: AtomicU64::new(0),
            expires_at_ms: AtomicU64::new(0),
            bypass_until_autocommit: AtomicBool::new(false),
        }
    }
}

struct QueueEventEmitter<'a> {
    conn: &'a Connection,
    config: QueueEventConfig,
    occurred_at: i64,
    last_offset: Option<i64>,
    emitted_events: u64,
}

/// Read an integer argument, accepting the REAL that dynamically typed
/// clients send for whole numbers.
///
/// `better-sqlite3` binds every JavaScript number as SQLite REAL, whole
/// ones included. So `honker_enqueue(..., priority, max_attempts, ...)`
/// called from Drizzle or Kysely arrives as REAL and used to fail with
/// "Invalid function parameter type Real". SQLite is dynamically typed
/// and these are integer arguments; refusing `3.0` because it arrived
/// as a double was our bug, not the caller's.
///
/// A REAL with a fractional part is still an error. Truncating `1.5` to
/// `1` would hide a real mistake, and a job id is not a rounding
/// candidate.
pub fn arg_i64(ctx: &Context<'_>, idx: usize) -> rusqlite::Result<i64> {
    match ctx.get_raw(idx) {
        ValueRef::Real(f) => real_to_i64(f, idx),
        // Integer, and everything else, keep rusqlite's own behavior.
        _ => ctx.get::<i64>(idx),
    }
}

/// Nullable form of [`arg_i64`]. NULL stays None.
pub fn arg_opt_i64(ctx: &Context<'_>, idx: usize) -> rusqlite::Result<Option<i64>> {
    match ctx.get_raw(idx) {
        ValueRef::Real(f) => real_to_i64(f, idx).map(Some),
        _ => ctx.get::<Option<i64>>(idx),
    }
}

fn real_to_i64(f: f64, idx: usize) -> rusqlite::Result<i64> {
    // 2^63 exactly; i64::MAX as f64 rounds *up* to it, so compare
    // against the power of two and exclude the top end.
    const LIMIT: f64 = 9_223_372_036_854_775_808.0;
    if f.fract() == 0.0 && (-LIMIT..LIMIT).contains(&f) {
        return Ok(f as i64);
    }

    // "Invalid function parameter type Real" is useless here: the type
    // is fine, the value is not. Say which value and why, because the
    // caller is often an ORM and the number came from somewhere else.
    let why = if f.is_nan() {
        "not a number".to_string()
    } else if f.is_infinite() {
        "infinite".to_string()
    } else if f.fract() != 0.0 {
        format!("{f} has a fractional part")
    } else {
        format!("{f:e} is outside the range of a 64-bit integer")
    };
    Err(rusqlite::Error::UserFunctionError(Box::new(
        std::io::Error::other(format!(
            "honker: argument {idx} must be a whole number, but {why}. \
             Whole values bind fine even when the client sends them as \
             REAL, which better-sqlite3 does for every JavaScript number."
        )),
    )))
}

/// Wrap a Displayable error for SQLite scalar-function returns.
fn to_sql_err<E: std::fmt::Display>(e: E) -> rusqlite::Error {
    rusqlite::Error::UserFunctionError(Box::new(std::io::Error::other(e.to_string())))
}

/// Register all `honker_*` honker scalar functions on `conn`. Idempotent
/// per-connection: creating the same function twice is a rusqlite
/// error, so call exactly once per connection.
pub fn attach_honker_functions(conn: &Connection) -> rusqlite::Result<()> {
    let queue_event_cache = Arc::new(QueueEventConfigCache::default());

    conn.create_scalar_function("honker_bootstrap", 0, FunctionFlags::SQLITE_UTF8, |ctx| {
        let db = unsafe { ctx.get_connection() }?;
        super::bootstrap_honker_schema(&db).map_err(to_sql_err)?;
        Ok(1i64)
    })?;

    let claim_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_claim_batch",
        4,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let queue: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let n: i64 = arg_i64(ctx, 2)?;
            let timeout_s: i64 = arg_i64(ctx, 3)?;
            let db = unsafe { ctx.get_connection() }?;
            claim_batch_with_cache(
                &db,
                &queue,
                &worker_id,
                n,
                timeout_s,
                Some(&claim_event_cache),
            )
            .map_err(to_sql_err)
        },
    )?;

    let ack_batch_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_ack_batch",
        2,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let ids_json: String = ctx.get(0)?;
            let worker_id: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            ack_batch_with_cache(&db, &ids_json, &worker_id, Some(&ack_batch_event_cache))
                .map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_queue_next_claim_at",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            queue_next_claim_at(&db, &queue).map_err(to_sql_err)
        },
    )?;

    let sweep_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_sweep_expired",
        1,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            sweep_expired_with_cache(&db, &queue, Some(&sweep_event_cache)).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_lock_acquire",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let ttl: i64 = arg_i64(ctx, 2)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_acquire(&db, &name, &owner, ttl).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_lock_release",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let owner: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            lock_release(&db, &name, &owner).map_err(to_sql_err)
        },
    )?;

    // honker_lock_renew(name, owner, ttl_s) -> 1 if this owner still
    // holds the lock and expires_at was extended, 0 otherwise.
    // Distinct from honker_lock_acquire: INSERT OR IGNORE does not
    // refresh expires_at for an existing (name, owner) row.
    conn.create_scalar_function("honker_lock_renew", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let name: String = ctx.get(0)?;
        let owner: String = ctx.get(1)?;
        let ttl: i64 = arg_i64(ctx, 2)?;
        let db = unsafe { ctx.get_connection() }?;
        lock_renew(&db, &name, &owner, ttl).map_err(to_sql_err)
    })?;

    conn.create_scalar_function(
        "honker_rate_limit_try",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let limit: i64 = arg_i64(ctx, 1)?;
            let per: i64 = arg_i64(ctx, 2)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_try(&db, &name, limit, per).map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function(
        "honker_rate_limit_sweep",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let older_than_s: i64 = arg_i64(ctx, 0)?;
            let db = unsafe { ctx.get_connection() }?;
            rate_limit_sweep(&db, older_than_s).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_register(name, queue, cron_expr, payload_json,
    //                       priority, expires_s_or_null) -> 1.
    // Optional 7th arg max_attempts (default 3) pins the attempt budget
    // on every job the scheduler enqueues for this task.
    // Upserts the task row. `next_fire_at` is recomputed as the next
    // cron boundary strictly after `unixepoch()`. Calling twice with
    // the same name replaces the first registration entirely.
    conn.create_scalar_function(
        "honker_scheduler_register",
        6,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let queue: String = ctx.get(1)?;
            let cron_expr: String = ctx.get(2)?;
            let payload: String = ctx.get(3)?;
            let priority: i64 = arg_i64(ctx, 4)?;
            let expires_s: Option<i64> = arg_opt_i64(ctx, 5)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_register(
                &db, &name, &queue, &cron_expr, &payload, priority, expires_s, 3,
            )
            .map_err(to_sql_err)
        },
    )?;
    conn.create_scalar_function(
        "honker_scheduler_register",
        7,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let queue: String = ctx.get(1)?;
            let cron_expr: String = ctx.get(2)?;
            let payload: String = ctx.get(3)?;
            let priority: i64 = arg_i64(ctx, 4)?;
            let expires_s: Option<i64> = arg_opt_i64(ctx, 5)?;
            let max_attempts: i64 = arg_i64(ctx, 6)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_register(
                &db,
                &name,
                &queue,
                &cron_expr,
                &payload,
                priority,
                expires_s,
                max_attempts,
            )
            .map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_unregister(name) -> rows deleted (0 or 1).
    conn.create_scalar_function(
        "honker_scheduler_unregister",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_unregister(&db, &name).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_tick(now_unix) -> JSON array of fires. For each
    // registered task whose `next_fire_at <= now`, enqueues the
    // payload into the task's queue, advances `next_fire_at` to the
    // next cron boundary, and appends `{name, queue, fire_at,
    // job_id}` to the output array. Caller typically holds
    // `_honker_locks` entry 'honker-scheduler' for mutual
    // exclusion across scheduler processes.
    let scheduler_tick_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_scheduler_tick",
        1,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let now_unix: i64 = arg_i64(ctx, 0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_tick_with_cache(&db, now_unix, Some(&scheduler_tick_event_cache))
                .map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_soonest() -> unix ts of the earliest next_fire_at
    // across all registered tasks, or 0 if no tasks. Scheduler main
    // loop uses this to compute its sleep duration.
    conn.create_scalar_function(
        "honker_scheduler_soonest",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            scheduler_soonest(&db).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_pause(name) / _resume(name) -> 1 if toggled, 0 otherwise.
    conn.create_scalar_function(
        "honker_scheduler_pause",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_pause(&db, &name).map_err(to_sql_err)
        },
    )?;
    conn.create_scalar_function(
        "honker_scheduler_resume",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_resume(&db, &name).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_list() -> JSON array of all schedules with state.
    conn.create_scalar_function(
        "honker_scheduler_list",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            scheduler_list(&db).map_err(to_sql_err)
        },
    )?;

    // honker_scheduler_update(name, cron_expr_or_null, payload_or_null,
    //                          priority_or_null, expires_s_or_null,
    //                          touch_expires) -> 1 if updated, 0 if missing.
    // Optional 8-arg form adds max_attempts_or_null, touch_max_attempts.
    // `touch_expires` is a 0/1 flag: when 1 we treat the expires_s arg
    // as the desired value (which may be NULL = "clear"); when 0 we
    // leave expires_s untouched. SQL has no good way to distinguish
    // "user passed NULL" from "user did not specify" otherwise. Same
    // pattern for max_attempts so old 6-arg raw callers stay compatible;
    // explicit NULL resets max_attempts to the scheduler default (3).
    conn.create_scalar_function(
        "honker_scheduler_update",
        6,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let cron_expr: Option<String> = ctx.get(1)?;
            let payload: Option<String> = ctx.get(2)?;
            let priority: Option<i64> = arg_opt_i64(ctx, 3)?;
            let expires_s_arg: Option<i64> = arg_opt_i64(ctx, 4)?;
            let touch_expires: i64 = arg_i64(ctx, 5)?;
            let db = unsafe { ctx.get_connection() }?;
            let expires_s = if touch_expires != 0 {
                Some(expires_s_arg)
            } else {
                None
            };
            scheduler_update(
                &db,
                &name,
                cron_expr.as_deref(),
                payload.as_deref(),
                priority,
                expires_s,
                None,
            )
            .map_err(to_sql_err)
        },
    )?;
    conn.create_scalar_function(
        "honker_scheduler_update",
        8,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let name: String = ctx.get(0)?;
            let cron_expr: Option<String> = ctx.get(1)?;
            let payload: Option<String> = ctx.get(2)?;
            let priority: Option<i64> = arg_opt_i64(ctx, 3)?;
            let expires_s_arg: Option<i64> = arg_opt_i64(ctx, 4)?;
            let touch_expires: i64 = arg_i64(ctx, 5)?;
            let max_attempts_arg: Option<i64> = arg_opt_i64(ctx, 6)?;
            let touch_max_attempts: i64 = arg_i64(ctx, 7)?;
            let db = unsafe { ctx.get_connection() }?;
            let expires_s = if touch_expires != 0 {
                Some(expires_s_arg)
            } else {
                None
            };
            let max_attempts = if touch_max_attempts != 0 {
                Some(max_attempts_arg)
            } else {
                None
            };
            scheduler_update(
                &db,
                &name,
                cron_expr.as_deref(),
                payload.as_deref(),
                priority,
                expires_s,
                max_attempts,
            )
            .map_err(to_sql_err)
        },
    )?;

    conn.create_scalar_function("honker_result_save", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let value: String = ctx.get(1)?;
        let ttl_s: i64 = arg_i64(ctx, 2)?;
        let db = unsafe { ctx.get_connection() }?;
        result_save(&db, job_id, &value, ttl_s).map_err(to_sql_err)
    })?;

    conn.create_scalar_function("honker_result_get", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let db = unsafe { ctx.get_connection() }?;
        result_get(&db, job_id).map_err(to_sql_err)
    })?;

    conn.create_scalar_function(
        "honker_result_sweep",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            result_sweep(&db).map_err(to_sql_err)
        },
    )?;

    // honker_enqueue(queue, payload, run_at_or_null, delay_or_null,
    //            priority, max_attempts, expires_or_null) -> inserted id.
    // Precedence: if `delay` is not NULL, use `unixepoch() + delay`;
    // else if `run_at` is not NULL, use that literal; else use
    // `unixepoch()`. `expires` is `unixepoch() + expires` if non-NULL,
    // else NULL (never expires).
    let enqueue_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_enqueue",
        7,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let queue: String = ctx.get(0)?;
            let payload: String = ctx.get(1)?;
            let run_at: Option<i64> = arg_opt_i64(ctx, 2)?;
            let delay: Option<i64> = arg_opt_i64(ctx, 3)?;
            let priority: i64 = arg_i64(ctx, 4)?;
            let max_attempts: i64 = arg_i64(ctx, 5)?;
            let expires: Option<i64> = arg_opt_i64(ctx, 6)?;
            let db = unsafe { ctx.get_connection() }?;
            enqueue_with_cache(
                &db,
                &queue,
                &payload,
                run_at,
                delay,
                priority,
                max_attempts,
                expires,
                Some(&enqueue_event_cache),
            )
            .map_err(to_sql_err)
        },
    )?;

    // honker_ack(job_id, worker_id) -> 1 if ack'd, 0 if claim expired /
    // not ours.
    let ack_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function("honker_ack", 2, FunctionFlags::SQLITE_UTF8, move |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        ack_with_cache(&db, job_id, &worker_id, Some(&ack_event_cache)).map_err(to_sql_err)
    })?;

    // honker_retry(job_id, worker_id, delay_s, error) -> 1 if retried /
    // moved to dead, 0 if not our claim. If attempts >= max_attempts,
    // moves the row to `_honker_dead` instead of flipping it back
    // to pending. Fires a notify on the queue's channel on successful
    // pending-flip (so waiting workers wake).
    let retry_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function("honker_retry", 4, FunctionFlags::SQLITE_UTF8, move |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let delay_s: i64 = arg_i64(ctx, 2)?;
        let error: String = ctx.get(3)?;
        let db = unsafe { ctx.get_connection() }?;
        retry_with_cache(
            &db,
            job_id,
            &worker_id,
            delay_s,
            &error,
            Some(&retry_event_cache),
        )
        .map_err(to_sql_err)
    })?;

    // honker_fail(job_id, worker_id, error) -> 1 if failed-to-dead, 0 if
    // not our claim.
    let fail_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function("honker_fail", 3, FunctionFlags::SQLITE_UTF8, move |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let error: String = ctx.get(2)?;
        let db = unsafe { ctx.get_connection() }?;
        fail_with_cache(&db, job_id, &worker_id, &error, Some(&fail_event_cache))
            .map_err(to_sql_err)
    })?;

    // honker_heartbeat(job_id, worker_id, extend_s) -> 1 if extended, 0
    // if not our claim.
    conn.create_scalar_function("honker_heartbeat", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let extend_s: i64 = arg_i64(ctx, 2)?;
        let db = unsafe { ctx.get_connection() }?;
        heartbeat(&db, job_id, &worker_id, extend_s).map_err(to_sql_err)
    })?;

    // honker_cancel(job_id) -> 1 if a pending/processing row was removed,
    // 0 otherwise. Idempotent on missing.
    let cancel_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function("honker_cancel", 1, FunctionFlags::SQLITE_UTF8, move |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let db = unsafe { ctx.get_connection() }?;
        cancel_with_cache(&db, job_id, Some(&cancel_event_cache)).map_err(to_sql_err)
    })?;

    // honker_get_job(job_id) -> JSON object on hit, empty string on miss.
    conn.create_scalar_function("honker_get_job", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let db = unsafe { ctx.get_connection() }?;
        get_job(&db, job_id).map_err(to_sql_err)
    })?;

    // honker_cron_next_after(expr, from_unix) -> unix_ts of next boundary
    // strictly after `from_unix`, minute precision, system local time.
    // Same 5-field grammar as standard Unix cron. Deterministic +
    // pure; marked DETERMINISTIC to let SQLite optimize inside joins.
    conn.create_scalar_function(
        "honker_cron_next_after",
        2,
        FunctionFlags::SQLITE_UTF8 | FunctionFlags::SQLITE_DETERMINISTIC,
        |ctx| {
            let expr: String = ctx.get(0)?;
            let from_unix: i64 = arg_i64(ctx, 1)?;
            super::cron::next_after_unix(&expr, from_unix).map_err(to_sql_err)
        },
    )?;

    // Stream functions. One impl for every binding; _honker_stream +
    // _honker_stream_consumers are the shared on-disk layout.

    // honker_stream_publish(topic, key_or_null, payload_json) -> offset.
    // INSERTs one event and fires a wake on honker:stream:<topic>.
    conn.create_scalar_function(
        "honker_stream_publish",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let topic: String = ctx.get(0)?;
            let key: Option<String> = ctx.get(1)?;
            let payload: String = ctx.get(2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_publish(&db, &topic, key.as_deref(), &payload).map_err(to_sql_err)
        },
    )?;

    // honker_stream_read_since(topic, offset, limit) -> JSON array of
    // {offset, topic, key, payload, created_at}.
    conn.create_scalar_function(
        "honker_stream_read_since",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let topic: String = ctx.get(0)?;
            let offset: i64 = arg_i64(ctx, 1)?;
            let limit: i64 = arg_i64(ctx, 2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_read_since(&db, &topic, offset, limit).map_err(to_sql_err)
        },
    )?;

    // honker_stream_save_offset(consumer, topic, offset) -> 1 if row
    // advanced (new row or higher offset), 0 if the saved offset is
    // already >= `offset`. Monotonic: never rewinds on duplicate
    // deliveries.
    conn.create_scalar_function(
        "honker_stream_save_offset",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let consumer: String = ctx.get(0)?;
            let topic: String = ctx.get(1)?;
            let offset: i64 = arg_i64(ctx, 2)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_save_offset(&db, &consumer, &topic, offset).map_err(to_sql_err)
        },
    )?;

    // honker_stream_get_offset(consumer, topic) -> offset or 0.
    conn.create_scalar_function(
        "honker_stream_get_offset",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let consumer: String = ctx.get(0)?;
            let topic: String = ctx.get(1)?;
            let db = unsafe { ctx.get_connection() }?;
            stream_get_offset(&db, &consumer, &topic).map_err(to_sql_err)
        },
    )?;

    // Queue lifecycle events are an opt-in, bounded stream maintained
    // transactionally by the queue mutation functions below.
    let configure_event_cache = Arc::clone(&queue_event_cache);
    conn.create_scalar_function(
        "honker_queue_events_configure",
        3,
        FunctionFlags::SQLITE_UTF8,
        move |ctx| {
            let enabled = arg_i64(ctx, 0)? != 0;
            let retention_target = arg_i64(ctx, 1)?;
            let include_payload = arg_i64(ctx, 2)? != 0;
            let db = unsafe { ctx.get_connection() }?;
            let configured =
                queue_events_configure(&db, enabled, retention_target, include_payload)
                    .map_err(to_sql_err)?;
            configure_event_cache.invalidate_after_configure(&db);
            Ok(configured)
        },
    )?;
    conn.create_scalar_function(
        "honker_queue_events_read_since",
        3,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let offset = arg_i64(ctx, 0)?;
            let queue: Option<String> = ctx.get(1)?;
            let limit = arg_i64(ctx, 2)?;
            let db = unsafe { ctx.get_connection() }?;
            queue_events_read_since(&db, offset, queue.as_deref(), limit).map_err(to_sql_err)
        },
    )?;
    conn.create_scalar_function(
        "honker_queue_events_status",
        0,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db = unsafe { ctx.get_connection() }?;
            queue_events_status(&db).map_err(to_sql_err)
        },
    )?;

    Ok(())
}

// ---------------------------------------------------------------------
// Claim / ack
// ---------------------------------------------------------------------

/// Move claimable rows that have already exhausted `max_attempts` into
/// `_honker_dead`. Without this, a worker that dies after the last
/// allowed claim leaves the row reclaimable forever — every reclaim
/// would bump `attempts` past `max_attempts` with no dead-letter path
/// (dead-letter previously only ran inside `retry()`).
///
/// "Claimable" here matches the reclaim predicate: pending+due or
/// processing with an expired visibility timeout. In-flight claims
/// that still hold a valid timeout are left alone so the holder can
/// still ack / retry / fail.
fn dead_letter_exhausted_claimable(
    conn: &Connection,
    queue: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let mut select = conn.prepare_cached(
        "DELETE FROM _honker_live
         WHERE queue = ?1
           AND attempts >= max_attempts
           AND (expires_at IS NULL OR expires_at > unixepoch())
           AND (
             (state = 'pending' AND run_at <= unixepoch())
             OR (state = 'processing' AND claim_expires_at < unixepoch())
           )
         RETURNING id, queue, payload, priority, run_at, max_attempts,
                   attempts, created_at",
    )?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, String, i64, i64, i64, i64, i64)> = select
        .query_map(rusqlite::params![queue], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'max attempts exceeded', ?8)",
    )?;
    let count = rows.len() as i64;
    let mut emitter = queue_event_emitter(conn, cache)?;
    for (id, queue, payload, priority, run_at, max_attempts, attempts, created_at) in rows {
        insert.execute(rusqlite::params![
            id,
            queue,
            payload,
            priority,
            run_at,
            max_attempts,
            attempts,
            created_at
        ])?;
        if let Some(emitter) = emitter.as_mut() {
            emitter.emit(QueueEventRecord {
                event_type: "dead_lettered",
                job_id: id,
                queue: &queue,
                payload: &payload,
                attempts,
                worker_id: None,
                run_at: Some(run_at),
                reason: Some("attempts_exhausted"),
                error: Some("max attempts exceeded"),
            })?;
        }
    }
    if let Some(emitter) = emitter {
        emitter.finish()?;
    }
    Ok(count)
}

/// Returns JSON text containing the complete claimed-job snapshot.
#[cfg(test)]
fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    claim_batch_with_cache(conn, queue, worker_id, n, timeout_s, None)
}

fn claim_batch_with_cache(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<String> {
    // Drop reclaimable rows that already used their attempt budget so
    // they cannot be claimed again (and so they don't clog the claim
    // index forever). Same outer SQL statement / connection, so this
    // shares the caller's transaction with the claim UPDATE below.
    dead_letter_exhausted_claimable(conn, queue, cache)?;

    let mut stmt = conn.prepare_cached(
        "UPDATE _honker_live
         SET state = 'processing',
             worker_id = ?1,
             claim_expires_at = unixepoch() + ?4,
             attempts = attempts + 1
         WHERE id IN (
           SELECT id FROM _honker_live
           WHERE queue = ?2
             AND state IN ('pending', 'processing')
             AND attempts < max_attempts
             AND (expires_at IS NULL OR expires_at > unixepoch())
             AND ((state = 'pending' AND run_at <= unixepoch())
               OR (state = 'processing' AND claim_expires_at < unixepoch()))
           ORDER BY priority DESC, run_at ASC, id ASC
           LIMIT ?3
         )
         RETURNING id, queue, payload, state, priority, run_at, worker_id,
                   claim_expires_at, attempts, max_attempts, created_at, expires_at",
    )?;
    #[allow(clippy::type_complexity)]
    let rows = stmt.query_map(rusqlite::params![worker_id, queue, n, timeout_s], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
            row.get::<_, String>(6)?,
            row.get::<_, i64>(7)?,
            row.get::<_, i64>(8)?,
            row.get::<_, i64>(9)?,
            row.get::<_, i64>(10)?,
            row.get::<_, Option<i64>>(11)?,
        ))
    })?;
    let mut out = Vec::new();
    // Three states distinguish "no claimed rows yet" from "rows exist but
    // events are disabled". That keeps empty worker polls free of config
    // reads while loading configuration only once for a non-empty batch.
    let mut emitter: Option<Option<QueueEventEmitter<'_>>> = None;
    for row in rows {
        let (
            id,
            q,
            payload,
            state,
            priority,
            run_at,
            w,
            claim_expires_at,
            attempts,
            max_attempts,
            created_at,
            expires_at,
        ) = row?;
        if emitter.is_none() {
            emitter = Some(queue_event_emitter(conn, cache)?);
        }
        if let Some(Some(emitter)) = emitter.as_mut() {
            emitter.emit(QueueEventRecord {
                event_type: "claimed",
                job_id: id,
                queue: &q,
                payload: &payload,
                attempts,
                worker_id: Some(&w),
                run_at: Some(run_at),
                reason: None,
                error: None,
            })?;
        }
        // payload stays a JSON string (double-encoded on the wire) so
        // every binding's existing parse path keeps working.
        out.push(json!({
            "id": id,
            "queue": q,
            "payload": payload,
            "state": state,
            "priority": priority,
            "run_at": run_at,
            "worker_id": w,
            "attempts": attempts,
            "claim_expires_at": claim_expires_at,
            "max_attempts": max_attempts,
            "created_at": created_at,
            "expires_at": expires_at,
        }));
    }
    if let Some(Some(emitter)) = emitter {
        emitter.finish()?;
    }
    Ok(Value::Array(out).to_string())
}

#[cfg(test)]
fn ack_batch(conn: &Connection, ids_json: &str, worker_id: &str) -> rusqlite::Result<i64> {
    ack_batch_with_cache(conn, ids_json, worker_id, None)
}

fn ack_batch_with_cache(
    conn: &Connection,
    ids_json: &str,
    worker_id: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let Some(mut emitter) = queue_event_emitter(conn, cache)? else {
        let deleted = conn.execute(
            "DELETE FROM _honker_live
             WHERE id IN (SELECT value FROM json_each(?1))
               AND worker_id = ?2
               AND claim_expires_at >= unixepoch()",
            rusqlite::params![ids_json, worker_id],
        )?;
        return Ok(deleted as i64);
    };

    #[allow(clippy::type_complexity)]
    let jobs: Vec<(i64, String, String, i64, i64, String)> = {
        let mut stmt = conn.prepare_cached(
            "DELETE FROM _honker_live
             WHERE id IN (SELECT value FROM json_each(?1))
               AND worker_id = ?2
               AND claim_expires_at >= unixepoch()
             RETURNING id, queue, payload, attempts, run_at, worker_id",
        )?;
        stmt.query_map(rusqlite::params![ids_json, worker_id], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    let count = jobs.len() as i64;
    for (id, queue, payload, attempts, run_at, worker_id) in jobs {
        emitter.emit(QueueEventRecord {
            event_type: "completed",
            job_id: id,
            queue: &queue,
            payload: &payload,
            attempts,
            worker_id: Some(&worker_id),
            run_at: Some(run_at),
            reason: None,
            error: None,
        })?;
    }
    emitter.finish()?;
    Ok(count)
}

/// Return the earliest future deadline that could make `claim_batch()`
/// return non-empty for this queue:
///   * a pending row's `run_at`
///   * one second after a processing row's `claim_expires_at`
///
/// Rows that have already exhausted `max_attempts` are ignored — they
/// are dead-lettered on the next claim path, not reclaimable.
///
/// Returns 0 if no such future deadline exists.
pub fn queue_next_claim_at(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT COALESCE(MIN(deadline), 0)
             FROM (
               SELECT MIN(run_at) AS deadline
               FROM _honker_live
               WHERE queue = ?1
                 AND state = 'pending'
                 AND attempts < max_attempts
                 AND (expires_at IS NULL OR expires_at > unixepoch())
                 AND run_at > unixepoch()
               UNION ALL
               SELECT MIN(claim_expires_at + 1) AS deadline
               FROM _honker_live
               WHERE queue = ?1
                 AND state = 'processing'
                 AND attempts < max_attempts
                 AND (expires_at IS NULL OR expires_at > unixepoch())
                 AND claim_expires_at >= unixepoch()
             )",
            rusqlite::params![queue],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Enqueue / single-job ack / retry / fail / heartbeat
// ---------------------------------------------------------------------

/// INSERT a job. Returns the new row's id.
///
/// Scheduling (lowest-to-highest precedence):
///   - no run_at, no delay → `unixepoch()` (claimable immediately)
///   - run_at set           → that literal unix timestamp
///   - delay set            → `unixepoch() + delay` (wins over run_at)
///
/// Expiration: NULL = never; `Some(s)` = `unixepoch() + s`.
#[cfg(test)]
fn enqueue(
    conn: &Connection,
    queue: &str,
    payload: &str,
    run_at: Option<i64>,
    delay: Option<i64>,
    priority: i64,
    max_attempts: i64,
    expires: Option<i64>,
) -> rusqlite::Result<i64> {
    enqueue_with_cache(
        conn,
        queue,
        payload,
        run_at,
        delay,
        priority,
        max_attempts,
        expires,
        None,
    )
}

#[allow(clippy::too_many_arguments)]
fn enqueue_with_cache(
    conn: &Connection,
    queue: &str,
    payload: &str,
    run_at: Option<i64>,
    delay: Option<i64>,
    priority: i64,
    max_attempts: i64,
    expires: Option<i64>,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let now: i64 = conn.query_row("SELECT unixepoch()", [], |r| r.get(0))?;
    let run_at_val: i64 = match (delay, run_at) {
        (Some(d), _) => now + d,
        (None, Some(r)) => r,
        (None, None) => now,
    };
    let expires_at: Option<i64> = expires.map(|e| now + e);

    // No synthetic `_honker_notifications` row. The live-table INSERT
    // already advances PRAGMA data_version on commit, which is what
    // SharedUpdateWatcher / every binding's update_events path observes.
    // Writing a wake row per enqueue used to grow the notifications
    // table without bound on high-throughput queues.
    let id: i64 = conn.query_row(
        "INSERT INTO _honker_live
           (queue, payload, run_at, priority, max_attempts, expires_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6)
         RETURNING id",
        rusqlite::params![
            queue,
            payload,
            run_at_val,
            priority,
            max_attempts,
            expires_at
        ],
        |r| r.get(0),
    )?;
    emit_queue_event(
        conn,
        cache,
        QueueEventRecord {
            event_type: "enqueued",
            job_id: id,
            queue,
            payload,
            attempts: 0,
            worker_id: None,
            run_at: Some(run_at_val),
            reason: None,
            error: None,
        },
    )?;
    Ok(id)
}

/// Single-job ack. DELETEs the row if the caller's claim is still
/// valid. Returns 1 on success, 0 if the claim expired or the row
/// isn't ours.
#[cfg(test)]
fn ack(conn: &Connection, job_id: i64, worker_id: &str) -> rusqlite::Result<i64> {
    ack_with_cache(conn, job_id, worker_id, None)
}

fn ack_with_cache(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let Some(mut emitter) = queue_event_emitter(conn, cache)? else {
        let deleted = conn.execute(
            "DELETE FROM _honker_live
             WHERE id = ?1 AND worker_id = ?2
               AND claim_expires_at >= unixepoch()",
            rusqlite::params![job_id, worker_id],
        )?;
        return Ok(deleted as i64);
    };

    let row: Option<(String, String, i64, i64, String)> = match conn.query_row(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND worker_id = ?2 AND claim_expires_at >= unixepoch()
         RETURNING queue, payload, attempts, run_at, worker_id",
        rusqlite::params![job_id, worker_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(error) => return Err(error),
    };
    let Some((queue, payload, attempts, run_at, worker_id)) = row else {
        return Ok(0);
    };
    emitter.emit(QueueEventRecord {
        event_type: "completed",
        job_id,
        queue: &queue,
        payload: &payload,
        attempts,
        worker_id: Some(&worker_id),
        run_at: Some(run_at),
        reason: None,
        error: None,
    })?;
    emitter.finish()?;
    Ok(1)
}

/// Retry or fail based on `attempts` vs `max_attempts`. If another
/// attempt is allowed, flips the row back to `'pending'` with
/// `run_at = unixepoch() + delay_s` and fires a wake. Otherwise
/// DELETEs from `_honker_live` and INSERTs into `_honker_dead`
/// with `last_error=error`.
///
/// Returns 1 if either branch ran, 0 if the claim is no longer valid
/// (expired / not our worker / row moved on).
#[cfg(test)]
fn retry(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
) -> rusqlite::Result<i64> {
    retry_with_cache(conn, job_id, worker_id, delay_s, error, None)
}

fn retry_with_cache(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, String, String, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "SELECT id, queue, payload, priority, run_at, max_attempts,
                    attempts, created_at
             FROM _honker_live
             WHERE id = ?1 AND worker_id = ?2
               AND claim_expires_at >= unixepoch()
               AND state = 'processing'",
            rusqlite::params![job_id, worker_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .ok();
    let Some((id, queue, payload, priority, run_at, max_attempts, attempts, created_at)) = row
    else {
        return Ok(0);
    };
    let (event_type, event_run_at) = if attempts >= max_attempts {
        conn.execute(
            "DELETE FROM _honker_live WHERE id = ?1",
            rusqlite::params![id],
        )?;
        conn.execute(
            "INSERT INTO _honker_dead
               (id, queue, payload, priority, run_at, max_attempts,
                attempts, last_error, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                id,
                queue,
                payload,
                priority,
                run_at,
                max_attempts,
                attempts,
                error,
                created_at
            ],
        )?;
        ("dead_lettered", run_at)
    } else {
        let retry_run_at = conn.query_row(
            "UPDATE _honker_live
             SET state = 'pending',
                 run_at = unixepoch() + ?2,
                 worker_id = NULL,
                 claim_expires_at = NULL
             WHERE id = ?1
             RETURNING run_at",
            rusqlite::params![id, delay_s],
            |r| r.get(0),
        )?;
        // Wake comes from the live-table UPDATE + commit (data_version).
        // No synthetic notification row — see enqueue() for rationale.
        ("retry_scheduled", retry_run_at)
    };
    emit_queue_event(
        conn,
        cache,
        QueueEventRecord {
            event_type,
            job_id: id,
            queue: &queue,
            payload: &payload,
            attempts,
            worker_id: Some(worker_id),
            run_at: Some(event_run_at),
            reason: (event_type == "dead_lettered").then_some("attempts_exhausted"),
            error: Some(error),
        },
    )?;
    Ok(1)
}

/// Unconditionally move the claim to `_honker_dead` with the given
/// error. Returns 1 if moved, 0 if not our claim.
fn fail_with_cache(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    error: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    #[allow(clippy::type_complexity)]
    let row: Option<(i64, String, String, i64, i64, i64, i64, i64)> = conn
        .query_row(
            "DELETE FROM _honker_live
             WHERE id = ?1 AND worker_id = ?2
               AND claim_expires_at >= unixepoch()
             RETURNING id, queue, payload, priority, run_at, max_attempts,
                       attempts, created_at",
            rusqlite::params![job_id, worker_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                ))
            },
        )
        .ok();
    let Some((id, queue, payload, priority, run_at, max_attempts, attempts, created_at)) = row
    else {
        return Ok(0);
    };
    conn.execute(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        rusqlite::params![
            id,
            queue,
            payload,
            priority,
            run_at,
            max_attempts,
            attempts,
            error,
            created_at
        ],
    )?;
    emit_queue_event(
        conn,
        cache,
        QueueEventRecord {
            event_type: "dead_lettered",
            job_id: id,
            queue: &queue,
            payload: &payload,
            attempts,
            worker_id: Some(worker_id),
            run_at: Some(run_at),
            reason: Some("explicit_failure"),
            error: Some(error),
        },
    )?;
    Ok(1)
}

/// Cancel a job by id. Removes pending or processing rows from
/// `_honker_live` regardless of which worker (if any) holds it.
/// Returns 1 if a row was removed, 0 otherwise. Idempotent.
///
/// Use case: an operator decides a queued or in-flight job is no
/// longer needed (the upstream request was cancelled, the user
/// changed their mind). Note that for a `state='processing'` row,
/// the worker holding the claim will see `ack()` return 0 on its
/// next call — same shape as a claim that simply expired.
#[cfg(test)]
fn cancel(conn: &Connection, job_id: i64) -> rusqlite::Result<i64> {
    cancel_with_cache(conn, job_id, None)
}

fn cancel_with_cache(
    conn: &Connection,
    job_id: i64,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let Some(mut emitter) = queue_event_emitter(conn, cache)? else {
        let deleted = conn.execute(
            "DELETE FROM _honker_live
             WHERE id = ?1 AND state IN ('pending', 'processing')",
            rusqlite::params![job_id],
        )?;
        return Ok(deleted as i64);
    };

    let row: Option<(String, String, i64, i64, Option<String>)> = match conn.query_row(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND state IN ('pending', 'processing')
         RETURNING queue, payload, attempts, run_at, worker_id",
        rusqlite::params![job_id],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
    ) {
        Ok(row) => Some(row),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(error) => return Err(error),
    };
    let Some((queue, payload, attempts, run_at, worker_id)) = row else {
        return Ok(0);
    };
    emitter.emit(QueueEventRecord {
        event_type: "cancelled",
        job_id,
        queue: &queue,
        payload: &payload,
        attempts,
        worker_id: worker_id.as_deref(),
        run_at: Some(run_at),
        reason: None,
        error: None,
    })?;
    emitter.finish()?;
    Ok(1)
}

/// Read a single job row by id. Returns a JSON object on success or
/// the empty string on miss (job ack'd, dead'd, or never existed).
/// Pure read — does not change state.
pub fn get_job(conn: &Connection, job_id: i64) -> rusqlite::Result<String> {
    let row: Option<(
        i64,
        String,
        String,
        String,
        i64,
        i64,
        Option<String>,
        Option<i64>,
        i64,
        i64,
        i64,
        Option<i64>,
    )> = conn
        .query_row(
            "SELECT id, queue, payload, state, priority, run_at, worker_id,
                    claim_expires_at, attempts, max_attempts, created_at, expires_at
               FROM _honker_live WHERE id = ?1",
            rusqlite::params![job_id],
            |r| {
                Ok((
                    r.get(0)?,
                    r.get(1)?,
                    r.get(2)?,
                    r.get(3)?,
                    r.get(4)?,
                    r.get(5)?,
                    r.get(6)?,
                    r.get(7)?,
                    r.get(8)?,
                    r.get(9)?,
                    r.get(10)?,
                    r.get(11)?,
                ))
            },
        )
        .ok();
    let Some((
        id,
        queue,
        payload,
        state,
        priority,
        run_at,
        worker_id,
        claim_expires_at,
        attempts,
        max_attempts,
        created_at,
        expires_at,
    )) = row
    else {
        return Ok(String::new());
    };
    Ok(json!({
        "id": id,
        "queue": queue,
        "payload": payload,
        "state": state,
        "priority": priority,
        "run_at": run_at,
        "worker_id": worker_id,
        "claim_expires_at": claim_expires_at,
        "attempts": attempts,
        "max_attempts": max_attempts,
        "created_at": created_at,
        "expires_at": expires_at,
    })
    .to_string())
}

/// Extend the current claim by `extend_s` seconds. Returns 1 if the
/// heartbeat landed, 0 if we're not the holder (either the row is
/// in a different state or worker_id doesn't match).
pub fn heartbeat(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    extend_s: i64,
) -> rusqlite::Result<i64> {
    // Require a still-valid claim. Without `claim_expires_at >= now`,
    // a late heartbeat after visibility timeout can steal the job
    // back from a reclaimer (dual execution).
    let updated = conn.execute(
        "UPDATE _honker_live
         SET claim_expires_at = unixepoch() + ?3
         WHERE id = ?1 AND worker_id = ?2 AND state = 'processing'
           AND claim_expires_at >= unixepoch()",
        rusqlite::params![job_id, worker_id, extend_s],
    )?;
    Ok(updated as i64)
}

// ---------------------------------------------------------------------
// Task expiration
// ---------------------------------------------------------------------

/// Move expired-pending rows from `_honker_live` to `_honker_dead`
/// with `last_error='expired'`. Returns count moved.
#[cfg(test)]
fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    sweep_expired_with_cache(conn, queue, None)
}

fn sweep_expired_with_cache(
    conn: &Connection,
    queue: &str,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<i64> {
    let mut select = conn.prepare_cached(
        "DELETE FROM _honker_live
         WHERE queue = ?1
           AND state = 'pending'
           AND expires_at IS NOT NULL
           AND expires_at <= unixepoch()
         RETURNING id, queue, payload, priority, run_at, max_attempts,
                   attempts, created_at",
    )?;
    #[allow(clippy::type_complexity)]
    let rows: Vec<(i64, String, String, i64, i64, i64, i64, i64)> = select
        .query_map(rusqlite::params![queue], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if rows.is_empty() {
        return Ok(0);
    }
    let mut insert = conn.prepare_cached(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, 'expired', ?8)",
    )?;
    let count = rows.len() as i64;
    let mut emitter = queue_event_emitter(conn, cache)?;
    for (id, queue, payload, priority, run_at, max_attempts, attempts, created_at) in rows {
        insert.execute(rusqlite::params![
            id,
            queue,
            payload,
            priority,
            run_at,
            max_attempts,
            attempts,
            created_at
        ])?;
        if let Some(emitter) = emitter.as_mut() {
            emitter.emit(QueueEventRecord {
                event_type: "dead_lettered",
                job_id: id,
                queue: &queue,
                payload: &payload,
                attempts,
                worker_id: None,
                run_at: Some(run_at),
                reason: Some("job_expired"),
                error: Some("expired"),
            })?;
        }
    }
    if let Some(emitter) = emitter {
        emitter.finish()?;
    }
    Ok(count)
}

// ---------------------------------------------------------------------
// Named locks
// ---------------------------------------------------------------------

pub fn lock_acquire(
    conn: &Connection,
    name: &str,
    owner: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    conn.execute(
        "DELETE FROM _honker_locks
         WHERE name = ?1 AND expires_at <= unixepoch()",
        rusqlite::params![name],
    )?;
    conn.execute(
        "INSERT OR IGNORE INTO _honker_locks (name, owner, expires_at)
         VALUES (?1, ?2, unixepoch() + ?3)",
        rusqlite::params![name, owner, ttl_s],
    )?;
    let current: Option<String> = conn
        .query_row(
            "SELECT owner FROM _honker_locks WHERE name = ?1",
            rusqlite::params![name],
            |r| r.get(0),
        )
        .ok();
    Ok(if current.as_deref() == Some(owner) {
        1
    } else {
        0
    })
}

pub fn lock_release(conn: &Connection, name: &str, owner: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_locks WHERE name = ?1 AND owner = ?2",
        rusqlite::params![name, owner],
    )?;
    Ok(deleted as i64)
}

/// Extend `expires_at` for a lock held by `owner`. Returns 1 if the
/// row was updated, 0 if the lock is missing or held by someone else.
///
/// `honker_lock_acquire` uses `INSERT OR IGNORE` and does **not**
/// refresh TTL on same-owner re-acquire — callers that need renewal
/// (scheduler leaders, long critical sections) must use this.
pub fn lock_renew(conn: &Connection, name: &str, owner: &str, ttl_s: i64) -> rusqlite::Result<i64> {
    if ttl_s <= 0 {
        return Err(to_sql_err("ttl_s must be positive"));
    }
    let updated = conn.execute(
        "UPDATE _honker_locks
         SET expires_at = unixepoch() + ?3
         WHERE name = ?1 AND owner = ?2",
        rusqlite::params![name, owner, ttl_s],
    )?;
    Ok(if updated > 0 { 1 } else { 0 })
}

// ---------------------------------------------------------------------
// Rate limiting
// ---------------------------------------------------------------------

pub fn rate_limit_try(
    conn: &Connection,
    name: &str,
    limit: i64,
    per: i64,
) -> rusqlite::Result<i64> {
    if limit <= 0 || per <= 0 {
        return Err(to_sql_err("limit and per must be positive"));
    }
    let window_start: i64 = conn.query_row(
        "SELECT (unixepoch() / ?1) * ?1",
        rusqlite::params![per],
        |r| r.get(0),
    )?;
    let changed = conn.execute(
        "INSERT INTO _honker_rate_limits (name, window_start, count)
         VALUES (?1, ?2, 1)
         ON CONFLICT(name, window_start) DO UPDATE SET count = count + 1
         WHERE count < ?3",
        rusqlite::params![name, window_start, limit],
    )?;
    Ok(if changed > 0 { 1 } else { 0 })
}

pub fn rate_limit_sweep(conn: &Connection, older_than_s: i64) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_rate_limits
         WHERE window_start < unixepoch() - ?1",
        rusqlite::params![older_than_s],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Scheduler state
// ---------------------------------------------------------------------

/// Register (or re-register) a periodic task. `next_fire_at` is
/// computed as the next cron boundary strictly after
/// `unixepoch()`. Calling twice with the same name replaces the
/// first registration entirely. `max_attempts` is stored on the task
/// row and applied to every job `scheduler_tick` enqueues for it.
pub fn scheduler_register(
    conn: &Connection,
    name: &str,
    queue: &str,
    cron_expr: &str,
    payload: &str,
    priority: i64,
    expires_s: Option<i64>,
    max_attempts: i64,
) -> rusqlite::Result<i64> {
    let max_attempts = if max_attempts < 1 { 1 } else { max_attempts };
    let now = now_unix(conn)?;
    let next_fire_at = super::cron::next_after_unix(cron_expr, now).map_err(to_sql_err)?;
    conn.execute(
        "INSERT INTO _honker_scheduler_tasks
           (name, queue, cron_expr, payload, priority, expires_s, next_fire_at, max_attempts)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)
         ON CONFLICT(name) DO UPDATE SET
           queue = excluded.queue,
           cron_expr = excluded.cron_expr,
           payload = excluded.payload,
           priority = excluded.priority,
           expires_s = excluded.expires_s,
           next_fire_at = excluded.next_fire_at,
           max_attempts = excluded.max_attempts",
        rusqlite::params![
            name,
            queue,
            cron_expr,
            payload,
            priority,
            expires_s,
            next_fire_at,
            max_attempts
        ],
    )?;
    // Wake any sleeping scheduler leader so it re-computes
    // honker_scheduler_soonest() against the new task set. Without
    // this, a leader that went to sleep for an hour before a newly-
    // registered 1-minute-from-now task existed would oversleep past
    // its first fire.
    //
    // Wake is the register/update write itself advancing data_version
    // on commit — see scheduler_wake.
    scheduler_wake(conn)?;
    Ok(1)
}

pub fn scheduler_unregister(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "DELETE FROM _honker_scheduler_tasks WHERE name = ?1",
        rusqlite::params![name],
    )?;
    if n > 0 {
        // Unregister can only make the "soonest" later, so a sleeping
        // leader wouldn't miss anything by oversleeping. But waking it
        // lets the loop observe the removal and notice if the table is
        // now empty (soonest() returns 0 → leader exits cleanly).
        scheduler_wake(conn)?;
    }
    Ok(n as i64)
}

/// Ensure a sleeping scheduler leader sitting on `update_events()`
/// re-evaluates after a register/unregister/pause/resume/update.
///
/// The register/unregister/pause/resume/update statements already
/// mutate `_honker_scheduler_tasks`, which advances data_version on
/// commit. A synthetic notification row used to be written here and
/// grew without bound under frequent schedule edits — no longer needed.
fn scheduler_wake(_conn: &Connection) -> rusqlite::Result<()> {
    Ok(())
}

/// Max fires enqueued for a single schedule row in one
/// `scheduler_tick` call. After a long outage an `@every 1s` task
/// would otherwise enqueue tens of thousands of jobs in one writer
/// transaction.
///
/// **Semantics (intentional):** once the cap is hit, remaining missed
/// boundaries for that task are **skipped** — `next_fire_at` jumps to
/// the next boundary strictly after `now_unix`. Those intermediate
/// fires are never enqueued. Run the scheduler continuously, use
/// coarser schedules, or raise this constant if every missed fire
/// must be delivered.
pub const SCHEDULER_MAX_CATCHUP_FIRES: i64 = 64;

/// For each registered task whose `next_fire_at <= now_unix`,
/// enqueue the payload into its queue and advance `next_fire_at`
/// to the next boundary. Keeps advancing within one tick while
/// boundaries remain in the past (catches up after a scheduler
/// outage), up to [`SCHEDULER_MAX_CATCHUP_FIRES`] per task.
/// Returns a JSON array of `{name, queue, fire_at, job_id}` fires.
fn scheduler_tick_with_cache(
    conn: &Connection,
    now_unix: i64,
    cache: Option<&QueueEventConfigCache>,
) -> rusqlite::Result<String> {
    #[allow(clippy::type_complexity)]
    let tasks: Vec<(String, String, String, String, i64, Option<i64>, i64, i64)> = {
        let mut stmt = conn.prepare_cached(
            "SELECT name, queue, cron_expr, payload, priority, expires_s,
                    next_fire_at, COALESCE(max_attempts, 3)
             FROM _honker_scheduler_tasks
             WHERE next_fire_at <= ?1 AND enabled = 1",
        )?;
        stmt.query_map(rusqlite::params![now_unix], |r| {
            Ok((
                r.get::<_, String>(0)?,
                r.get::<_, String>(1)?,
                r.get::<_, String>(2)?,
                r.get::<_, String>(3)?,
                r.get::<_, i64>(4)?,
                r.get::<_, Option<i64>>(5)?,
                r.get::<_, i64>(6)?,
                r.get::<_, i64>(7)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?
    };
    let mut out = Vec::new();
    for (name, queue, cron_expr, payload, priority, expires_s, mut next_fire_at, max_attempts) in
        tasks
    {
        let mut fires_this_task: i64 = 0;
        while next_fire_at <= now_unix {
            if fires_this_task >= SCHEDULER_MAX_CATCHUP_FIRES {
                // Skip the remaining backlog. Resume from the next
                // boundary strictly after now so we don't immediately
                // re-enter the catch-up loop on the next tick.
                // Intermediate boundaries are intentionally never
                // enqueued (see SCHEDULER_MAX_CATCHUP_FIRES docs).
                next_fire_at =
                    super::cron::next_after_unix(&cron_expr, now_unix).map_err(to_sql_err)?;
                break;
            }
            // Enqueue at this boundary. `run_at` is NULL (claimable
            // immediately); `expires` is the task's expires_s if set.
            // max_attempts comes from the schedule row, not a constant.
            let job_id = enqueue_with_cache(
                conn,
                &queue,
                &payload,
                None,
                None,
                priority,
                max_attempts,
                expires_s,
                cache,
            )?;
            out.push(json!({
                "name": name,
                "queue": queue,
                "fire_at": next_fire_at,
                "job_id": job_id,
            }));
            fires_this_task += 1;
            // Advance to the next boundary strictly after this one.
            next_fire_at =
                super::cron::next_after_unix(&cron_expr, next_fire_at).map_err(to_sql_err)?;
        }
        // Persist the advanced next_fire_at.
        conn.execute(
            "UPDATE _honker_scheduler_tasks
             SET next_fire_at = ?2 WHERE name = ?1",
            rusqlite::params![name, next_fire_at],
        )?;
    }
    Ok(Value::Array(out).to_string())
}

pub fn scheduler_soonest(conn: &Connection) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT COALESCE(MIN(next_fire_at), 0) FROM _honker_scheduler_tasks WHERE enabled = 1",
            [],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

/// Toggle `enabled` on a registered schedule. Returns 1 if updated, 0
/// if the name doesn't exist. Wakes the leader so `scheduler_soonest`
/// is recomputed against the new active set.
pub fn scheduler_pause(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "UPDATE _honker_scheduler_tasks SET enabled = 0 WHERE name = ?1 AND enabled = 1",
        rusqlite::params![name],
    )?;
    if n > 0 {
        scheduler_wake(conn)?;
    }
    Ok(n as i64)
}

pub fn scheduler_resume(conn: &Connection, name: &str) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "UPDATE _honker_scheduler_tasks SET enabled = 1 WHERE name = ?1 AND enabled = 0",
        rusqlite::params![name],
    )?;
    if n > 0 {
        scheduler_wake(conn)?;
    }
    Ok(n as i64)
}

/// Return all registered schedules as a JSON array. Each row:
/// `{name, queue, cron_expr, payload, priority, expires_s,
///   next_fire_at, enabled, max_attempts}`.
pub fn scheduler_list(conn: &Connection) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare(
        "SELECT name, queue, cron_expr, payload, priority, expires_s,
                next_fire_at, enabled, COALESCE(max_attempts, 3)
           FROM _honker_scheduler_tasks
           ORDER BY name",
    )?;
    let rows: Vec<(
        String,
        String,
        String,
        String,
        i64,
        Option<i64>,
        i64,
        i64,
        i64,
    )> = stmt
        .query_map([], |r| {
            Ok((
                r.get(0)?,
                r.get(1)?,
                r.get(2)?,
                r.get(3)?,
                r.get(4)?,
                r.get(5)?,
                r.get(6)?,
                r.get(7)?,
                r.get(8)?,
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;
    let mut out = Vec::new();
    for (
        name,
        queue,
        cron_expr,
        payload,
        priority,
        expires_s,
        next_fire_at,
        enabled,
        max_attempts,
    ) in rows
    {
        out.push(json!({
            "name": name,
            "queue": queue,
            "cron_expr": cron_expr,
            "payload": payload,
            "priority": priority,
            "expires_s": expires_s,
            "next_fire_at": next_fire_at,
            "enabled": enabled != 0,
            "max_attempts": max_attempts,
        }));
    }
    Ok(Value::Array(out).to_string())
}

/// Mutate one or more fields of a registered schedule. Pass `None` for
/// fields that should be left unchanged. If `cron_expr` is provided,
/// `next_fire_at` is recomputed from `unixepoch()`. Returns 1 if the
/// row was updated, 0 if it doesn't exist.
#[allow(clippy::too_many_arguments)]
pub fn scheduler_update(
    conn: &Connection,
    name: &str,
    cron_expr: Option<&str>,
    payload: Option<&str>,
    priority: Option<i64>,
    expires_s: Option<Option<i64>>,
    max_attempts: Option<Option<i64>>,
) -> rusqlite::Result<i64> {
    // Verify exists first so we can return 0 cleanly without dynamic SQL gymnastics.
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM _honker_scheduler_tasks WHERE name = ?1",
            rusqlite::params![name],
            |_| Ok(true),
        )
        .unwrap_or(false);
    if !exists {
        return Ok(0);
    }
    let any_field = cron_expr.is_some()
        || payload.is_some()
        || priority.is_some()
        || expires_s.is_some()
        || max_attempts.is_some();
    if !any_field {
        // No fields to change. Don't wake the leader for a no-op.
        return Ok(0);
    }
    // Wrap field UPDATEs in a SAVEPOINT so a concurrent reader can't
    // observe half-applied state. SAVEPOINT instead of BEGIN/COMMIT so
    // we play nicely if the caller already holds an outer tx.
    let next_fire_at = if let Some(expr) = cron_expr {
        let now = now_unix(conn)?;
        Some(super::cron::next_after_unix(expr, now).map_err(to_sql_err)?)
    } else {
        None
    };
    conn.execute_batch("SAVEPOINT honker_sched_update")?;
    let result: rusqlite::Result<()> = (|| {
        if let Some(p) = payload {
            conn.execute(
                "UPDATE _honker_scheduler_tasks SET payload = ?2 WHERE name = ?1",
                rusqlite::params![name, p],
            )?;
        }
        if let Some(p) = priority {
            conn.execute(
                "UPDATE _honker_scheduler_tasks SET priority = ?2 WHERE name = ?1",
                rusqlite::params![name, p],
            )?;
        }
        if let Some(e) = expires_s {
            conn.execute(
                "UPDATE _honker_scheduler_tasks SET expires_s = ?2 WHERE name = ?1",
                rusqlite::params![name, e],
            )?;
        }
        if let Some(m) = max_attempts {
            let m = m.unwrap_or(3);
            let m = if m < 1 { 1 } else { m };
            conn.execute(
                "UPDATE _honker_scheduler_tasks SET max_attempts = ?2 WHERE name = ?1",
                rusqlite::params![name, m],
            )?;
        }
        if let Some(expr) = cron_expr {
            conn.execute(
                "UPDATE _honker_scheduler_tasks
                   SET cron_expr = ?2, next_fire_at = ?3 WHERE name = ?1",
                rusqlite::params![name, expr, next_fire_at.unwrap()],
            )?;
        }
        Ok(())
    })();
    if result.is_err() {
        let _ = conn.execute_batch(
            "ROLLBACK TO SAVEPOINT honker_sched_update; \
                                    RELEASE SAVEPOINT honker_sched_update",
        );
        result?;
    }
    conn.execute_batch("RELEASE SAVEPOINT honker_sched_update")?;
    scheduler_wake(conn)?;
    Ok(1)
}

// ---------------------------------------------------------------------
// Task result storage
// ---------------------------------------------------------------------

pub fn result_save(
    conn: &Connection,
    job_id: i64,
    value: &str,
    ttl_s: i64,
) -> rusqlite::Result<i64> {
    if ttl_s > 0 {
        conn.execute(
            "INSERT INTO _honker_results (job_id, value, expires_at)
             VALUES (?1, ?2, unixepoch() + ?3)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = excluded.expires_at",
            rusqlite::params![job_id, value, ttl_s],
        )?;
    } else {
        conn.execute(
            "INSERT INTO _honker_results (job_id, value, expires_at)
             VALUES (?1, ?2, NULL)
             ON CONFLICT(job_id) DO UPDATE
               SET value = excluded.value,
                   expires_at = NULL",
            rusqlite::params![job_id, value],
        )?;
    }
    Ok(1)
}

pub fn result_get(conn: &Connection, job_id: i64) -> rusqlite::Result<Option<String>> {
    let row: Option<(Option<String>, Option<i64>)> = conn
        .query_row(
            "SELECT value, expires_at FROM _honker_results WHERE job_id = ?1",
            rusqlite::params![job_id],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .ok();
    match row {
        None => Ok(None),
        Some((_, Some(exp))) if exp <= now_unix(conn)? => Ok(None),
        Some((value, _)) => Ok(value),
    }
}

pub fn result_sweep(conn: &Connection) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_results
         WHERE expires_at IS NOT NULL AND expires_at <= unixepoch()",
        [],
    )?;
    Ok(deleted as i64)
}

// ---------------------------------------------------------------------
// Streams
// ---------------------------------------------------------------------

pub fn queue_events_configure(
    conn: &Connection,
    enabled: bool,
    retention_target: i64,
    include_payload: bool,
) -> rusqlite::Result<i64> {
    if !(1..=1_000_000).contains(&retention_target) {
        return Err(to_sql_err(
            "honker: queue event retention_target must be between 1 and 1000000",
        ));
    }
    conn.execute(
        "INSERT INTO _honker_queue_event_config
           (singleton, enabled, retention_target, include_payload, events_since_trim)
         VALUES (1, ?1, ?2, ?3, 0)
         ON CONFLICT(singleton) DO UPDATE SET
           enabled = excluded.enabled,
           retention_target = excluded.retention_target,
           include_payload = excluded.include_payload,
           events_since_trim = 0",
        rusqlite::params![enabled as i64, retention_target, include_payload as i64],
    )?;
    if let Some(trim_through) = trim_queue_events(conn, retention_target)? {
        conn.execute(
            "UPDATE _honker_queue_event_config
             SET trimmed_through_offset = MAX(trimmed_through_offset, ?1)
             WHERE singleton = 1",
            [trim_through],
        )?;
    }
    Ok(1)
}

pub fn queue_events_status(conn: &Connection) -> rusqlite::Result<String> {
    let mut schema_available = true;
    let config = match conn.query_row(
        "SELECT enabled, retention_target, include_payload, trimmed_through_offset
             FROM _honker_queue_event_config WHERE singleton = 1",
        [],
        |row| {
            Ok((
                row.get::<_, i64>(0)? != 0,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)? != 0,
                row.get::<_, i64>(3)?,
            ))
        },
    ) {
        Ok(config) => Some(config),
        Err(rusqlite::Error::QueryReturnedNoRows) => None,
        Err(error) => {
            if queue_event_config_table_exists(conn)? {
                return Err(error);
            }
            schema_available = false;
            None
        }
    };
    let (enabled, retention_target, include_payload, trimmed_through_offset) =
        config.unwrap_or((false, 10_000, false, 0));
    let (oldest_offset, latest_offset): (Option<i64>, Option<i64>) = conn.query_row(
        "SELECT MIN(offset), MAX(offset) FROM _honker_stream WHERE topic = ?1",
        [QUEUE_EVENTS_TOPIC],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )?;
    Ok(json!({
        "schema_available": schema_available,
        "enabled": enabled,
        "retention_target": retention_target,
        "include_payload": include_payload,
        "trimmed_through_offset": trimmed_through_offset,
        "oldest_offset": oldest_offset,
        "latest_offset": latest_offset,
    })
    .to_string())
}

fn queue_events_trimmed_through(conn: &Connection) -> rusqlite::Result<i64> {
    match conn.query_row(
        "SELECT trimmed_through_offset
         FROM _honker_queue_event_config WHERE singleton = 1",
        [],
        |row| row.get(0),
    ) {
        Ok(offset) => Ok(offset),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(0),
        Err(error) => Err(error),
    }
}

fn load_queue_event_config(conn: &Connection) -> rusqlite::Result<Option<QueueEventConfig>> {
    let result = (|| {
        let mut stmt = conn.prepare_cached(
            "SELECT retention_target, include_payload
             FROM _honker_queue_event_config
             WHERE singleton = 1 AND enabled = 1",
        )?;
        stmt.query_row([], |r| {
            Ok(QueueEventConfig {
                retention_target: r.get(0)?,
                include_payload: r.get::<_, i64>(1)? != 0,
            })
        })
    })();
    match result {
        Ok(config) => Ok(Some(config)),
        Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
        Err(error) => {
            // A newer core can share a database with an older binding that
            // has not created the opt-in config table yet. Queue mutations
            // must remain backwards-compatible in that mixed-version case;
            // the absence of configuration means events are disabled.
            if !queue_event_config_table_exists(conn)? {
                Ok(None)
            } else {
                Err(error)
            }
        }
    }
}

fn queue_event_config_table_exists(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(
        "SELECT EXISTS(
           SELECT 1 FROM sqlite_schema
           WHERE type = 'table' AND name = '_honker_queue_event_config'
         )",
        [],
        |row| Ok(row.get::<_, i64>(0)? != 0),
    )
}

impl QueueEventConfigCache {
    fn get(&self, conn: &Connection) -> rusqlite::Result<Option<QueueEventConfig>> {
        if self.bypass_until_autocommit.load(Ordering::Acquire) {
            if conn.is_autocommit() {
                self.bypass_until_autocommit.store(false, Ordering::Release);
                self.invalidate();
            } else {
                return load_queue_event_config(conn);
            }
        }

        let now_ms = queue_event_cache_millis();
        if now_ms < self.expires_at_ms.load(Ordering::Acquire) {
            let encoded = self.encoded.load(Ordering::Relaxed);
            return Ok(decode_queue_event_config(encoded));
        }

        let config = load_queue_event_config(conn)?;
        self.encoded
            .store(encode_queue_event_config(config), Ordering::Relaxed);
        self.expires_at_ms.store(
            now_ms + QUEUE_EVENT_CONFIG_CACHE_TTL.as_millis() as u64,
            Ordering::Release,
        );
        Ok(config)
    }

    fn invalidate_after_configure(&self, conn: &Connection) {
        self.invalidate();
        // A configure call inside an explicit transaction may still roll
        // back. Do not cache its uncommitted value until autocommit resumes.
        self.bypass_until_autocommit
            .store(!conn.is_autocommit(), Ordering::Release);
    }

    fn invalidate(&self) {
        self.expires_at_ms.store(0, Ordering::Release);
        self.encoded.store(0, Ordering::Relaxed);
    }
}

fn queue_event_trim_interval(retention_target: i64) -> u64 {
    ((retention_target as u64) / 10).clamp(1, 1_000)
}

fn queue_event_cache_millis() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START
        .get_or_init(Instant::now)
        .elapsed()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

fn encode_queue_event_config(config: Option<QueueEventConfig>) -> u64 {
    match config {
        None => 0,
        Some(config) => {
            let payload_bit = u64::from(config.include_payload) << 63;
            payload_bit | config.retention_target as u64
        }
    }
}

fn decode_queue_event_config(encoded: u64) -> Option<QueueEventConfig> {
    if encoded == 0 {
        return None;
    }
    Some(QueueEventConfig {
        retention_target: (encoded & i64::MAX as u64) as i64,
        include_payload: encoded >> 63 == 1,
    })
}

fn queue_event_emitter<'a>(
    conn: &'a Connection,
    cache: Option<&'a QueueEventConfigCache>,
) -> rusqlite::Result<Option<QueueEventEmitter<'a>>> {
    let config = match cache {
        Some(cache) => cache.get(conn)?,
        None => load_queue_event_config(conn)?,
    };
    let Some(config) = config else {
        return Ok(None);
    };
    Ok(Some(QueueEventEmitter {
        conn,
        config,
        occurred_at: now_unix(conn)?,
        last_offset: None,
        emitted_events: 0,
    }))
}

fn emit_queue_event(
    conn: &Connection,
    cache: Option<&QueueEventConfigCache>,
    event: QueueEventRecord<'_>,
) -> rusqlite::Result<()> {
    let Some(mut emitter) = queue_event_emitter(conn, cache)? else {
        return Ok(());
    };
    emitter.emit(event)?;
    emitter.finish()
}

impl QueueEventEmitter<'_> {
    fn emit(&mut self, event: QueueEventRecord<'_>) -> rusqlite::Result<()> {
        let mut body = json!({
            "version": 1,
            "type": event.event_type,
            "job_id": event.job_id,
            "queue": event.queue,
            "occurred_at": self.occurred_at,
            "attempts": event.attempts,
            "worker_id": event.worker_id,
            "run_at": event.run_at,
            "reason": event.reason,
            "error": event.error,
        });
        if self.config.include_payload {
            let payload = serde_json::from_str(event.payload)
                .unwrap_or_else(|_| Value::String(event.payload.to_string()));
            body.as_object_mut()
                .expect("queue event body is always an object")
                .insert("payload".to_string(), payload);
        }

        self.last_offset = Some(stream_publish_internal(
            self.conn,
            QUEUE_EVENTS_TOPIC,
            Some(event.queue),
            &body.to_string(),
        )?);
        self.emitted_events += 1;
        Ok(())
    }

    fn finish(self) -> rusqlite::Result<()> {
        if self.last_offset.is_none() {
            return Ok(());
        }

        // Count emissions in the database, not in a connection-local cache.
        // Queue writers commonly open short-lived connections and may run in
        // several processes; the singleton row makes trim scheduling global,
        // serialized with the queue mutation, and rollback-safe. Batch
        // mutations still update the counter only once.
        let (events_since_trim, retention_target): (i64, i64) = self.conn.query_row(
            "UPDATE _honker_queue_event_config
             SET events_since_trim = events_since_trim + ?1
             WHERE singleton = 1
             RETURNING events_since_trim, retention_target",
            [self.emitted_events as i64],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        let interval = queue_event_trim_interval(retention_target) as i64;
        if events_since_trim < interval {
            return Ok(());
        }

        let trim_through = trim_queue_events(self.conn, retention_target)?;
        if let Some(trim_through) = trim_through {
            self.conn.execute(
                "UPDATE _honker_queue_event_config
                 SET events_since_trim = events_since_trim % ?1,
                     trimmed_through_offset = MAX(trimmed_through_offset, ?2)
                 WHERE singleton = 1",
                rusqlite::params![interval, trim_through],
            )?;
        } else {
            self.conn.execute(
                "UPDATE _honker_queue_event_config
                 SET events_since_trim = events_since_trim % ?1
                 WHERE singleton = 1",
                [interval],
            )?;
        }
        Ok(())
    }
}

/// Delete queue events beyond the configured target and return the newest
/// deleted offset. Offsets are global to all stream topics, so both the
/// threshold lookup and deletion must explicitly select the reserved topic.
fn trim_queue_events(conn: &Connection, retention_target: i64) -> rusqlite::Result<Option<i64>> {
    let trim_through = {
        let mut stmt = conn.prepare_cached(
            "SELECT offset FROM _honker_stream
             WHERE topic = ?1
             ORDER BY offset DESC
             LIMIT 1 OFFSET ?2",
        )?;
        match stmt.query_row(
            rusqlite::params![QUEUE_EVENTS_TOPIC, retention_target],
            |row| row.get::<_, i64>(0),
        ) {
            Ok(offset) => Some(offset),
            Err(rusqlite::Error::QueryReturnedNoRows) => None,
            Err(error) => return Err(error),
        }
    };
    if let Some(trim_through) = trim_through {
        let mut delete =
            conn.prepare_cached("DELETE FROM _honker_stream WHERE topic = ?1 AND offset <= ?2")?;
        delete.execute(rusqlite::params![QUEUE_EVENTS_TOPIC, trim_through])?;
    }
    Ok(trim_through)
}

pub fn queue_events_read_since(
    conn: &Connection,
    offset: i64,
    queue: Option<&str>,
    limit: i64,
) -> rusqlite::Result<String> {
    if offset < 0 {
        return Err(to_sql_err(
            "honker: queue event offset must be greater than or equal to zero",
        ));
    }
    if !(1..=10_000).contains(&limit) {
        return Err(to_sql_err(
            "honker: queue event read limit must be between 1 and 10000",
        ));
    }

    let trimmed_through = queue_events_trimmed_through(conn)?;
    if offset < trimmed_through {
        return Err(to_sql_err(format!(
            "HONKER_QUEUE_EVENT_OFFSET_EXPIRED: requested offset {offset} is older than \
             trimmed-through offset {trimmed_through}"
        )));
    }

    let mut stmt = conn.prepare_cached(
        "SELECT offset, payload
         FROM _honker_stream
         WHERE topic = ?1 AND offset > ?2
           AND (?3 IS NULL OR key = ?3)
         ORDER BY offset ASC
         LIMIT ?4",
    )?;
    let rows = stmt.query_map(
        rusqlite::params![QUEUE_EVENTS_TOPIC, offset, queue, limit],
        |r| Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?)),
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (event_offset, payload) = row?;
        let mut event: Value = serde_json::from_str(&payload).map_err(to_sql_err)?;
        event
            .as_object_mut()
            .ok_or_else(|| to_sql_err("honker: stored queue event is not an object"))?
            .insert("offset".to_string(), json!(event_offset));
        out.push(event);
    }
    Ok(Value::Array(out).to_string())
}

pub fn stream_publish(
    conn: &Connection,
    topic: &str,
    key: Option<&str>,
    payload: &str,
) -> rusqlite::Result<i64> {
    if topic == QUEUE_EVENTS_TOPIC {
        return Err(to_sql_err(
            "honker: topic '_honker:queue-events:v1' is reserved for queue lifecycle events",
        ));
    }
    stream_publish_internal(conn, topic, key, payload)
}

fn stream_publish_internal(
    conn: &Connection,
    topic: &str,
    key: Option<&str>,
    payload: &str,
) -> rusqlite::Result<i64> {
    // Stream row INSERT advances data_version on commit — same wake
    // path as enqueue. No synthetic notification row (see enqueue).
    let mut stmt = conn.prepare_cached(
        "INSERT INTO _honker_stream (topic, key, payload)
         VALUES (?1, ?2, ?3) RETURNING offset",
    )?;
    let offset: i64 = stmt.query_row(rusqlite::params![topic, key, payload], |r| r.get(0))?;
    Ok(offset)
}

/// Returns JSON: `[{"offset":N,"topic":"t","key":"k_or_null","payload":"...","created_at":T}, ...]`.
/// `key` is a raw JSON token — `null` for SQL NULL, otherwise a JSON
/// string literal.
pub fn stream_read_since(
    conn: &Connection,
    topic: &str,
    offset: i64,
    limit: i64,
) -> rusqlite::Result<String> {
    let mut stmt = conn.prepare_cached(
        "SELECT offset, topic, key, payload, created_at
         FROM _honker_stream
         WHERE topic = ?1 AND offset > ?2
         ORDER BY offset ASC
         LIMIT ?3",
    )?;
    let rows = stmt.query_map(rusqlite::params![topic, offset, limit], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?,
            r.get::<_, String>(3)?,
            r.get::<_, i64>(4)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (off, top, key, payload, created_at) = row?;
        out.push(json!({
            "offset": off,
            "topic": top,
            "key": key,
            "payload": payload,
            "created_at": created_at,
        }));
    }
    Ok(Value::Array(out).to_string())
}

pub fn stream_save_offset(
    conn: &Connection,
    consumer: &str,
    topic: &str,
    offset: i64,
) -> rusqlite::Result<i64> {
    // Monotonic upsert: WHERE excluded.offset > existing. The CHANGES
    // pragma reports affected rows, which we translate to 1/0.
    let changed = conn.execute(
        "INSERT INTO _honker_stream_consumers (name, topic, offset)
         VALUES (?1, ?2, ?3)
         ON CONFLICT(name, topic) DO UPDATE SET offset = excluded.offset
           WHERE excluded.offset > _honker_stream_consumers.offset",
        rusqlite::params![consumer, topic, offset],
    )?;
    Ok(if changed > 0 { 1 } else { 0 })
}

pub fn stream_get_offset(conn: &Connection, consumer: &str, topic: &str) -> rusqlite::Result<i64> {
    Ok(conn
        .query_row(
            "SELECT offset FROM _honker_stream_consumers
             WHERE name = ?1 AND topic = ?2",
            rusqlite::params![consumer, topic],
            |r| r.get(0),
        )
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn now_unix(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
}

#[cfg(test)]
mod real_arg_tests {
    use crate::{attach_honker_functions, bootstrap_honker_schema};
    use rusqlite::Connection;
    use serde_json::Value;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        conn
    }

    // better-sqlite3 binds every JavaScript number as REAL, so this is
    // the exact shape a Drizzle or Kysely caller sends. Before the
    // arg_i64 coercion this failed with "Invalid function parameter
    // type Real at index 4".
    #[test]
    fn enqueue_accepts_real_priority_and_max_attempts() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, ?1, ?2, NULL)",
                (0.0_f64, 3.0_f64),
                |r| r.get(0),
            )
            .unwrap();
        assert!(id > 0);
    }

    #[test]
    fn claim_batch_returns_the_complete_job_snapshot() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{\"to\":\"alice@example.com\"}',
                                       NULL, NULL, 7, 5, 600)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let claimed: String = conn
            .query_row(
                "SELECT honker_claim_batch('emails', 'worker-1', 1, 300)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let rows: Value = serde_json::from_str(&claimed).unwrap();
        let job = &rows[0];

        assert_eq!(job["id"], id);
        assert_eq!(job["queue"], "emails");
        assert_eq!(job["payload"], r#"{"to":"alice@example.com"}"#);
        assert_eq!(job["state"], "processing");
        assert_eq!(job["priority"], 7);
        assert_eq!(job["worker_id"], "worker-1");
        assert_eq!(job["attempts"], 1);
        assert_eq!(job["max_attempts"], 5);
        assert!(job["run_at"].as_i64().unwrap() > 0);
        assert!(job["claim_expires_at"].as_i64().unwrap() > 0);
        assert!(job["created_at"].as_i64().unwrap() > 0);
        assert!(job["expires_at"].as_i64().unwrap() > 0);
    }

    #[test]
    fn ack_accepts_a_real_job_id() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let _: String = conn
            .query_row(
                "SELECT honker_claim_batch('emails', 'w1', 8, 300)",
                [],
                |r| r.get(0),
            )
            .unwrap();

        let acked: i64 = conn
            .query_row("SELECT honker_ack(?1, 'w1')", [id as f64], |r| r.get(0))
            .unwrap();
        assert_eq!(acked, 1, "a REAL job id must ack the same row");
    }

    // Coercion is not rounding. A fractional argument is a caller
    // mistake and has to stay an error.
    #[test]
    fn fractional_reals_are_still_rejected() {
        let conn = db();
        let err = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, ?1, 3, NULL)",
                [1.5_f64],
                |r| r.get::<_, i64>(0),
            )
            .unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("must be a whole number") && msg.contains("fractional part"),
            "expected a message naming the value and why, got: {err}"
        );
    }

    #[test]
    fn null_optional_args_stay_none() {
        let conn = db();
        // expires is the argument where None and Some(0) actually
        // differ in the stored row: `expires.map(|e| now + e)` writes
        // NULL for None and `now` for Some(0). run_at and delay both
        // collapse to `now` either way, so neither can discriminate.
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let expires_at: Option<i64> = conn
            .query_row(
                "SELECT expires_at FROM _honker_live WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            expires_at, None,
            "a NULL expires argument must stay None, not become Some(0)"
        );
    }

    #[test]
    fn whole_real_delay_coerces() {
        let conn = db();
        let now: i64 = conn
            .query_row("SELECT unixepoch()", [], |r| r.get(0))
            .unwrap();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, ?1, 0, 3, NULL)",
                [30.0_f64],
                |r| r.get(0),
            )
            .unwrap();
        let run_at: i64 = conn
            .query_row("SELECT run_at FROM _honker_live WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(
            (run_at - (now + 30)).abs() <= 1,
            "a REAL delay of 30.0 must schedule now + 30, got {run_at} (now = {now})"
        );
    }

    // The bounds in real_to_i64 are the part most likely to be wrong,
    // so pin every edge rather than trusting the comparison reads right.
    #[test]
    fn real_bounds_are_exact() {
        let conn = db();
        let probe = |v: f64| -> Result<i64, rusqlite::Error> {
            conn.query_row("SELECT honker_ack(?1, 'w1')", [v], |r| r.get::<_, i64>(0))
        };

        // 2^63 is not representable as i64; i64::MAX as f64 rounds up to
        // it, which is why the check is `f < LIMIT` and not `<=`.
        assert!(
            probe(9_223_372_036_854_775_808.0).is_err(),
            "2^63 must reject"
        );
        // Largest f64 strictly below 2^63.
        assert!(
            probe(9_223_372_036_854_774_784.0).is_ok(),
            "2^63-1024 must pass"
        );
        // -2^63 is exactly i64::MIN and must pass.
        assert!(
            probe(-9_223_372_036_854_775_808.0).is_ok(),
            "-2^63 must pass"
        );
        assert!(probe(f64::INFINITY).is_err(), "infinity must reject");
        assert!(probe(f64::NEG_INFINITY).is_err(), "-infinity must reject");
        assert!(probe(f64::NAN).is_err(), "NaN must reject");
        // Negative zero is whole and must coerce to 0, not error.
        assert!(probe(-0.0).is_ok(), "-0.0 must pass");
    }
}

#[cfg(test)]
mod queue_event_tests {
    use super::*;
    use crate::{attach_honker_functions, bootstrap_honker_schema};
    use rusqlite::OpenFlags;
    use std::thread;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        conn
    }

    fn events(conn: &Connection, queue: Option<&str>) -> Vec<Value> {
        serde_json::from_str(&queue_events_read_since(conn, 0, queue, 10_000).unwrap()).unwrap()
    }

    fn event_status(conn: &Connection) -> Value {
        serde_json::from_str(&queue_events_status(conn).unwrap()).unwrap()
    }

    fn sql_enqueue(conn: &Connection, queue: &str) -> i64 {
        conn.query_row(
            "SELECT honker_enqueue(?1, '{}', NULL, NULL, 0, 3, NULL)",
            [queue],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[test]
    fn queue_events_are_disabled_by_default() {
        let conn = db();
        enqueue(&conn, "emails", "{}", None, None, 0, 3, None).unwrap();
        assert!(events(&conn, None).is_empty());
    }

    #[test]
    fn queue_mutations_remain_compatible_with_pre_event_schema() {
        let conn = db();
        conn.execute("DROP TABLE _honker_queue_event_config", [])
            .unwrap();

        let status = event_status(&conn);
        assert_eq!(status["schema_available"], false);
        assert_eq!(status["enabled"], false);

        let id = enqueue(&conn, "emails", "{}", None, None, 0, 3, None).unwrap();
        claim_batch(&conn, "emails", "legacy-worker", 1, 300).unwrap();
        assert_eq!(ack(&conn, id, "legacy-worker").unwrap(), 1);
    }

    #[test]
    fn queue_event_config_cache_refreshes_across_connections() {
        let uri = format!(
            "file:honker-queue-event-cache-{}?mode=memory&cache=shared",
            std::process::id()
        );
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI;
        let first = Connection::open_with_flags(&uri, flags).unwrap();
        let second = Connection::open_with_flags(&uri, flags).unwrap();
        attach_honker_functions(&first).unwrap();
        attach_honker_functions(&second).unwrap();
        bootstrap_honker_schema(&first).unwrap();

        sql_enqueue(&first, "before-enable");
        second
            .query_row("SELECT honker_queue_events_configure(1, 100, 0)", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();
        thread::sleep(QUEUE_EVENT_CONFIG_CACHE_TTL + Duration::from_millis(25));
        let enabled_id = sql_enqueue(&first, "after-enable");
        assert!(
            events(&first, None)
                .iter()
                .any(|event| { event["job_id"] == enabled_id && event["type"] == "enqueued" })
        );

        second
            .query_row("SELECT honker_queue_events_configure(0, 100, 0)", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();
        thread::sleep(QUEUE_EVENT_CONFIG_CACHE_TTL + Duration::from_millis(25));
        let last_offset = events(&first, None).last().unwrap()["offset"]
            .as_i64()
            .unwrap();
        sql_enqueue(&first, "after-disable");
        assert!(
            serde_json::from_str::<Vec<Value>>(
                &queue_events_read_since(&first, last_offset, None, 100).unwrap()
            )
            .unwrap()
            .is_empty()
        );
    }

    #[test]
    fn queue_event_config_cache_encodes_the_largest_retention() {
        for include_payload in [false, true] {
            let config = QueueEventConfig {
                retention_target: i64::MAX,
                include_payload,
            };
            let decoded = decode_queue_event_config(encode_queue_event_config(Some(config)))
                .expect("enabled configuration should round-trip");
            assert_eq!(decoded.retention_target, i64::MAX);
            assert_eq!(decoded.include_payload, include_payload);
        }
        assert!(decode_queue_event_config(encode_queue_event_config(None)).is_none());
    }

    #[test]
    fn rolled_back_configuration_does_not_poison_connection_cache() {
        let conn = db();
        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        conn.query_row("SELECT honker_queue_events_configure(1, 100, 0)", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();
        sql_enqueue(&conn, "rolled-back-config");
        conn.execute_batch("ROLLBACK").unwrap();

        let id = sql_enqueue(&conn, "still-disabled");
        assert!(
            events(&conn, None)
                .iter()
                .all(|event| event["job_id"] != id)
        );
    }

    #[test]
    fn queue_event_topic_rejects_public_stream_writes() {
        let conn = db();
        assert!(stream_publish(&conn, QUEUE_EVENTS_TOPIC, None, "{}").is_err());

        queue_events_configure(&conn, true, 100, false).unwrap();
        let id = enqueue(&conn, "internal", "{}", None, None, 0, 3, None).unwrap();
        assert!(
            events(&conn, None)
                .iter()
                .any(|event| { event["job_id"] == id && event["type"] == "enqueued" })
        );
    }

    #[test]
    fn queue_events_follow_committed_lifecycle_transitions() {
        let conn = db();
        queue_events_configure(&conn, true, 100, false).unwrap();

        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        let rolled_back = enqueue(
            &conn,
            "emails",
            "{\"rolled_back\":true}",
            None,
            None,
            0,
            3,
            None,
        )
        .unwrap();
        conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT COUNT(*) FROM _honker_live WHERE id = ?1",
                [rolled_back],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
        assert!(events(&conn, None).is_empty());

        let id = enqueue(
            &conn,
            "emails",
            "{\"to\":\"alice@example.com\"}",
            None,
            None,
            7,
            3,
            None,
        )
        .unwrap();
        claim_batch(&conn, "emails", "worker-1", 1, 300).unwrap();
        assert_eq!(ack(&conn, id, "not-the-owner").unwrap(), 0);
        retry(&conn, id, "worker-1", 0, "temporary").unwrap();
        claim_batch(&conn, "emails", "worker-2", 1, 300).unwrap();
        ack(&conn, id, "worker-2").unwrap();

        let retained = events(&conn, None);
        let types: Vec<&str> = retained
            .iter()
            .map(|event| event["type"].as_str().unwrap())
            .collect();
        assert_eq!(
            types,
            vec![
                "enqueued",
                "claimed",
                "retry_scheduled",
                "claimed",
                "completed"
            ]
        );
        assert!(retained.iter().all(|event| event.get("payload").is_none()));
        assert!(
            retained
                .windows(2)
                .all(|pair| pair[0]["offset"].as_i64() < pair[1]["offset"].as_i64())
        );
    }

    #[test]
    fn queue_events_cover_terminal_and_batch_transitions() {
        let conn = db();
        queue_events_configure(&conn, true, 100, false).unwrap();

        let dead_id = enqueue(&conn, "jobs", "{}", None, None, 0, 1, None).unwrap();
        claim_batch(&conn, "jobs", "worker-1", 1, 300).unwrap();
        retry(&conn, dead_id, "worker-1", 0, "permanent").unwrap();

        let cancel_id = enqueue(&conn, "jobs", "{}", None, None, 0, 3, None).unwrap();
        cancel(&conn, cancel_id).unwrap();

        let batch_a = enqueue(&conn, "batch", "{}", None, None, 0, 3, None).unwrap();
        let batch_b = enqueue(&conn, "batch", "{}", None, None, 0, 3, None).unwrap();
        claim_batch(&conn, "batch", "worker-batch", 2, 300).unwrap();
        assert_eq!(
            ack_batch(
                &conn,
                &json!([batch_a, batch_b]).to_string(),
                "worker-batch"
            )
            .unwrap(),
            2
        );

        let retained = events(&conn, None);
        assert!(retained.iter().any(|event| {
            event["job_id"] == dead_id
                && event["type"] == "dead_lettered"
                && event["reason"] == "attempts_exhausted"
        }));
        assert!(
            retained
                .iter()
                .any(|event| { event["job_id"] == cancel_id && event["type"] == "cancelled" })
        );
        assert_eq!(
            retained
                .iter()
                .filter(|event| event["type"] == "completed")
                .count(),
            2
        );

        let expired_id = enqueue(&conn, "jobs", "{}", None, None, 0, 3, Some(0)).unwrap();
        assert_eq!(sweep_expired(&conn, "jobs").unwrap(), 1);

        let abandoned_id = enqueue(&conn, "jobs", "{}", None, None, 0, 1, None).unwrap();
        claim_batch(&conn, "jobs", "abandoned-worker", 1, 300).unwrap();
        conn.execute(
            "UPDATE _honker_live
             SET claim_expires_at = unixepoch() - 1
             WHERE id = ?1",
            [abandoned_id],
        )
        .unwrap();
        assert!(
            serde_json::from_str::<Vec<Value>>(
                &claim_batch(&conn, "jobs", "next-worker", 1, 300).unwrap()
            )
            .unwrap()
            .is_empty()
        );

        let terminal = events(&conn, None);
        assert!(terminal.iter().any(|event| {
            event["job_id"] == expired_id
                && event["type"] == "dead_lettered"
                && event["reason"] == "job_expired"
                && event["error"] == "expired"
        }));
        assert!(terminal.iter().any(|event| {
            event["job_id"] == abandoned_id
                && event["type"] == "dead_lettered"
                && event["reason"] == "attempts_exhausted"
                && event["error"] == "max attempts exceeded"
        }));
    }

    #[test]
    fn scheduler_enqueues_emit_through_the_cached_core_path() {
        let conn = db();
        queue_events_configure(&conn, true, 100, false).unwrap();
        conn.query_row(
            "SELECT honker_scheduler_register(
               'scheduled-event', 'scheduled', '* * * * *', '{}', 0, NULL
             )",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
        let next_fire: i64 = conn
            .query_row(
                "SELECT next_fire_at FROM _honker_scheduler_tasks
                 WHERE name = 'scheduled-event'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.query_row("SELECT honker_scheduler_tick(?1)", [next_fire], |r| {
            r.get::<_, String>(0)
        })
        .unwrap();

        assert!(
            events(&conn, Some("scheduled"))
                .iter()
                .any(|event| { event["type"] == "enqueued" && event["queue"] == "scheduled" })
        );
    }

    #[test]
    fn queue_events_are_bounded_filterable_and_optionally_include_payloads() {
        let conn = db();
        queue_events_configure(&conn, true, 3, true).unwrap();

        for i in 0..5 {
            let queue = if i % 2 == 0 { "alpha" } else { "beta" };
            enqueue(
                &conn,
                queue,
                &json!({ "sequence": i }).to_string(),
                None,
                None,
                0,
                3,
                None,
            )
            .unwrap();
            stream_publish(
                &conn,
                "application-events",
                None,
                &json!({ "i": i }).to_string(),
            )
            .unwrap();
        }

        let status = event_status(&conn);
        let trimmed_through = status["trimmed_through_offset"].as_i64().unwrap();
        assert!(trimmed_through > 0);
        assert!(queue_events_read_since(&conn, 0, None, 10_000).is_err());
        let retained: Vec<Value> = serde_json::from_str(
            &queue_events_read_since(&conn, trimmed_through, None, 10_000).unwrap(),
        )
        .unwrap();
        assert_eq!(retained.len(), 3);
        assert_eq!(retained[0]["payload"]["sequence"], 2);
        assert_eq!(retained[2]["payload"]["sequence"], 4);

        let alpha: Vec<Value> = serde_json::from_str(
            &queue_events_read_since(&conn, trimmed_through, Some("alpha"), 10_000).unwrap(),
        )
        .unwrap();
        assert_eq!(alpha.len(), 2);
        assert!(alpha.iter().all(|event| event["queue"] == "alpha"));
    }

    #[test]
    fn production_retention_trims_in_bounded_chunks() {
        let conn = db();
        conn.query_row("SELECT honker_queue_events_configure(1, 100, 0)", [], |r| {
            r.get::<_, i64>(0)
        })
        .unwrap();

        for _ in 0..105 {
            sql_enqueue(&conn, "chunked");
        }
        let before_trim: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_stream WHERE topic = ?1",
                [QUEUE_EVENTS_TOPIC],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(before_trim, 105);

        for _ in 0..5 {
            sql_enqueue(&conn, "chunked");
        }
        let status = event_status(&conn);
        let after_trim: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_stream WHERE topic = ?1",
                [QUEUE_EVENTS_TOPIC],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(after_trim, 100);
        assert!(status["trimmed_through_offset"].as_i64().unwrap() > 0);
    }

    #[test]
    fn retention_accounting_survives_connection_churn() {
        let uri = format!(
            "file:honker-queue-event-retention-{}?mode=memory&cache=shared",
            std::process::id()
        );
        let flags = OpenFlags::SQLITE_OPEN_READ_WRITE
            | OpenFlags::SQLITE_OPEN_CREATE
            | OpenFlags::SQLITE_OPEN_URI;
        let keeper = Connection::open_with_flags(&uri, flags).unwrap();
        attach_honker_functions(&keeper).unwrap();
        bootstrap_honker_schema(&keeper).unwrap();
        keeper
            .query_row("SELECT honker_queue_events_configure(1, 20, 0)", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap();

        // Model request-scoped and multi-process clients: every mutation gets
        // a fresh SQLite connection and therefore a fresh config cache.
        for _ in 0..45 {
            let writer = Connection::open_with_flags(&uri, flags).unwrap();
            attach_honker_functions(&writer).unwrap();
            sql_enqueue(&writer, "short-lived-writers");
        }

        let retained: i64 = keeper
            .query_row(
                "SELECT COUNT(*) FROM _honker_stream WHERE topic = ?1",
                [QUEUE_EVENTS_TOPIC],
                |r| r.get(0),
            )
            .unwrap();
        let (events_since_trim, trimmed_through): (i64, i64) = keeper
            .query_row(
                "SELECT events_since_trim, trimmed_through_offset
                 FROM _honker_queue_event_config WHERE singleton = 1",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(retained, 21);
        assert_eq!(events_since_trim, 1);
        assert!(trimmed_through > 0);
        assert!(queue_events_read_since(&keeper, 0, None, 100).is_err());
    }

    #[test]
    fn retention_counter_is_transactional() {
        let conn = db();
        queue_events_configure(&conn, true, 100, false).unwrap();

        conn.execute_batch("BEGIN IMMEDIATE").unwrap();
        sql_enqueue(&conn, "rolled-back-counter");
        assert_eq!(
            conn.query_row(
                "SELECT events_since_trim FROM _honker_queue_event_config",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
            1
        );
        conn.execute_batch("ROLLBACK").unwrap();
        assert_eq!(
            conn.query_row(
                "SELECT events_since_trim FROM _honker_queue_event_config",
                [],
                |r| r.get::<_, i64>(0),
            )
            .unwrap(),
            0
        );
    }

    #[test]
    fn lowering_retention_trims_existing_events_immediately() {
        let conn = db();
        queue_events_configure(&conn, true, 100, false).unwrap();
        for _ in 0..5 {
            enqueue(&conn, "reconfigured", "{}", None, None, 0, 3, None).unwrap();
        }

        queue_events_configure(&conn, true, 3, false).unwrap();
        let status = event_status(&conn);
        let retained: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM _honker_stream WHERE topic = ?1",
                [QUEUE_EVENTS_TOPIC],
                |r| r.get(0),
            )
            .unwrap();
        let events_since_trim: i64 = conn
            .query_row(
                "SELECT events_since_trim FROM _honker_queue_event_config",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(retained, 3);
        assert_eq!(events_since_trim, 0);
        assert!(status["trimmed_through_offset"].as_i64().unwrap() > 0);
    }
}
