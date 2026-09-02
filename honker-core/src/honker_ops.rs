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
use rusqlite::OptionalExtension;
use rusqlite::functions::{Context, FunctionFlags};
use rusqlite::types::ValueRef;
use serde_json::{Value, json};

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

/// Run `body` inside `SAVEPOINT <name>` and undo it if anything fails.
///
/// Five honker operations destroy a row and then do more work with what
/// came back: `DELETE ... RETURNING` decodes the returned columns, and
/// the dead-letter paths follow the DELETE with an INSERT into
/// `_honker_dead`. Measured on issue #133: without a savepoint a failure
/// in that second half leaves the DELETE committed and the job in
/// neither `_honker_live` nor `_honker_dead` — silent job loss.
///
/// SAVEPOINT rather than BEGIN/COMMIT: these functions run inside the
/// caller's statement and the caller may already hold a transaction.
/// SAVEPOINT nests; BEGIN does not.
///
/// The error handling is the point, so it is spelled out:
///
///   * The undo result is never discarded. A connection left in an
///     unknown state has to be loud, and swallowing it is the exact bug
///     #133 is about.
///   * `body`'s error wins when the undo also fails — it is the cause —
///     and the undo failure is appended to its message rather than
///     replacing it.
///   * If RELEASE fails, the transaction this savepoint opened is still
///     open. We undo it before returning, so a caller never gets an
///     error *and* a connection silently left mid-transaction. Bindings
///     hold long-lived connections; every later call would otherwise
///     join that transaction.
fn in_savepoint<T>(
    conn: &Connection,
    name: &str,
    body: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    // True means this SAVEPOINT is what opens the transaction, so we
    // own it and nobody else's work is inside it.
    let owns_transaction = conn.is_autocommit();
    conn.execute_batch(&format!("SAVEPOINT {name}"))?;
    match body() {
        Ok(value) => match conn.execute_batch(&format!("RELEASE SAVEPOINT {name}")) {
            Ok(()) => Ok(value),
            // RELEASE of the outermost savepoint is the COMMIT. If it
            // fails (a lock conflict on a rollback-journal database,
            // say) the write is not durable, so roll it back instead of
            // leaving it dangling.
            Err(release_err) => Err(undo_savepoint(conn, name, owns_transaction, release_err)),
        },
        Err(body_err) => Err(undo_savepoint(conn, name, owns_transaction, body_err)),
    }
}

/// Undo the savepoint opened by [`in_savepoint`] and return the error to
/// hand back to the caller.
fn undo_savepoint(
    conn: &Connection,
    name: &str,
    owns_transaction: bool,
    cause: rusqlite::Error,
) -> rusqlite::Error {
    if conn.is_autocommit() {
        // SQLite already rolled the whole transaction back on its own —
        // an ABORT, SQLITE_FULL or an I/O error does that. There is no
        // savepoint left to roll back to, and issuing one would turn a
        // clean state into a bogus second error.
        return cause;
    }

    let mut failures: Vec<String> = Vec::new();

    // Undo our own frame and pop it, leaving any outer transaction the
    // caller opened untouched. This is also the form that is legal from
    // inside a scalar function: every one of these operations is reached
    // as `SELECT honker_*(...)`, and SQLite refuses a bare
    // COMMIT/ROLLBACK while the outer statement is still stepping.
    if let Err(err) = conn.execute_batch(&format!(
        "ROLLBACK TO SAVEPOINT {name}; RELEASE SAVEPOINT {name}"
    )) {
        failures.push(format!("ROLLBACK TO SAVEPOINT {name} failed: {err}"));
    }

    // For the outermost savepoint that RELEASE is the COMMIT, so it can
    // fail exactly the way the first one did (a lock conflict on a
    // rollback-journal database). If we opened the transaction, nobody
    // else's work is inside it and a plain ROLLBACK is the way out.
    // Without this the caller gets an error *and* a connection silently
    // left mid-transaction — bindings hold long-lived connections, so
    // every later call would join it.
    if owns_transaction && !conn.is_autocommit() {
        match conn.execute_batch("ROLLBACK") {
            // Back to a known state: nothing was committed and the
            // connection is out of the transaction. Whatever went wrong
            // above is no longer an unknown state, so don't dress the
            // caller's error up with it.
            Ok(()) => failures.clear(),
            Err(err) => failures.push(format!("ROLLBACK failed: {err}")),
        }
    }

    if failures.is_empty() {
        return cause;
    }
    // Never swallow a failed undo: the connection is in an unknown state
    // and that has to be loud. Keep the cause's text intact — callers
    // and tests match on it — and append what else went wrong.
    to_sql_err(format!(
        "{cause}; additionally, undoing SAVEPOINT {name} left the connection \
         in an unknown state: {}",
        failures.join("; ")
    ))
}

/// Register all `honker_*` honker scalar functions on `conn`. Idempotent
/// per-connection: creating the same function twice is a rusqlite
/// error, so call exactly once per connection.
pub fn attach_honker_functions(conn: &Connection) -> rusqlite::Result<()> {
    conn.create_scalar_function("honker_bootstrap", 0, FunctionFlags::SQLITE_UTF8, |ctx| {
        let db = unsafe { ctx.get_connection() }?;
        super::bootstrap_honker_schema(&db).map_err(to_sql_err)?;
        Ok(1i64)
    })?;

    conn.create_scalar_function("honker_claim_batch", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let queue: String = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let n: i64 = arg_i64(ctx, 2)?;
        let timeout_s: i64 = arg_i64(ctx, 3)?;
        let db = unsafe { ctx.get_connection() }?;
        claim_batch(&db, &queue, &worker_id, n, timeout_s).map_err(to_sql_err)
    })?;

    conn.create_scalar_function("honker_ack_batch", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let ids_json: String = ctx.get(0)?;
        let worker_id: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        ack_batch(&db, &ids_json, &worker_id).map_err(to_sql_err)
    })?;

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

    conn.create_scalar_function(
        "honker_sweep_expired",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let queue: String = ctx.get(0)?;
            let db = unsafe { ctx.get_connection() }?;
            sweep_expired(&db, &queue).map_err(to_sql_err)
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
    conn.create_scalar_function(
        "honker_scheduler_tick",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let now_unix: i64 = arg_i64(ctx, 0)?;
            let db = unsafe { ctx.get_connection() }?;
            scheduler_tick(&db, now_unix).map_err(to_sql_err)
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
    conn.create_scalar_function("honker_enqueue", 7, FunctionFlags::SQLITE_UTF8, |ctx| {
        let queue: String = ctx.get(0)?;
        let payload: String = ctx.get(1)?;
        let run_at: Option<i64> = arg_opt_i64(ctx, 2)?;
        let delay: Option<i64> = arg_opt_i64(ctx, 3)?;
        let priority: i64 = arg_i64(ctx, 4)?;
        let max_attempts: i64 = arg_i64(ctx, 5)?;
        let expires: Option<i64> = arg_opt_i64(ctx, 6)?;
        let db = unsafe { ctx.get_connection() }?;
        enqueue(
            &db,
            &queue,
            &payload,
            run_at,
            delay,
            priority,
            max_attempts,
            expires,
        )
        .map_err(to_sql_err)
    })?;

    // honker_ack(job_id, worker_id) -> 1 if ack'd, 0 if claim expired /
    // not ours.
    conn.create_scalar_function("honker_ack", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let db = unsafe { ctx.get_connection() }?;
        ack(&db, job_id, &worker_id).map_err(to_sql_err)
    })?;

    // honker_retry(job_id, worker_id, delay_s, error) -> 1 if retried /
    // moved to dead, 0 if not our claim. If attempts >= max_attempts,
    // moves the row to `_honker_dead` instead of flipping it back
    // to pending. Fires a notify on the queue's channel on successful
    // pending-flip (so waiting workers wake).
    conn.create_scalar_function("honker_retry", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let delay_s: i64 = arg_i64(ctx, 2)?;
        let error: String = ctx.get(3)?;
        let db = unsafe { ctx.get_connection() }?;
        retry(&db, job_id, &worker_id, delay_s, &error).map_err(to_sql_err)
    })?;

    // honker_fail(job_id, worker_id, error) -> 1 if failed-to-dead, 0 if
    // not our claim.
    conn.create_scalar_function("honker_fail", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let error: String = ctx.get(2)?;
        let db = unsafe { ctx.get_connection() }?;
        fail(&db, job_id, &worker_id, &error).map_err(to_sql_err)
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
    conn.create_scalar_function("honker_cancel", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let db = unsafe { ctx.get_connection() }?;
        cancel(&db, job_id).map_err(to_sql_err)
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
///
/// Runs in a SAVEPOINT. The DELETE uses RETURNING and the rows are only
/// decoded and re-inserted afterwards, so a decode or INSERT failure
/// would otherwise leave the whole matching batch in neither table.
/// Measured before the fix: one bad `attempts` value lost every row the
/// DELETE matched (live=0, dead=0 for all of them). This one runs on
/// every ordinary claim, so it is the most reachable of the five.
fn dead_letter_exhausted_claimable(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_dead_letter_claimable", || {
        dead_letter_exhausted_claimable_inner(conn, queue)
    })
}

fn dead_letter_exhausted_claimable_inner(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
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
    for r in rows {
        insert.execute(rusqlite::params![r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7])?;
    }
    Ok(count)
}

/// Returns JSON text: `[{"id":1,"queue":"...","payload":"...","worker_id":"...","attempts":N,"claim_expires_at":T}, ...]`
pub fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    // Drop reclaimable rows that already used their attempt budget so
    // they cannot be claimed again (and so they don't clog the claim
    // index forever). Same outer SQL statement / connection, so this
    // shares the caller's transaction with the claim UPDATE below.
    dead_letter_exhausted_claimable(conn, queue)?;

    // No SAVEPOINT around this one. It is an UPDATE, not a DELETE: a
    // decode failure in the mapper below leaves the row in
    // `_honker_live` with `attempts` bumped and the claim held, so the
    // job is still there and becomes reclaimable when the visibility
    // timeout expires. Nothing is lost, so there is nothing to undo.
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
         RETURNING id, queue, payload, worker_id, attempts, claim_expires_at",
    )?;
    let rows = stmt.query_map(rusqlite::params![worker_id, queue, n, timeout_s], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, String>(1)?,
            row.get::<_, String>(2)?,
            row.get::<_, String>(3)?,
            row.get::<_, i64>(4)?,
            row.get::<_, i64>(5)?,
        ))
    })?;
    let mut out = Vec::new();
    for row in rows {
        let (id, q, payload, w, attempts, claim_expires_at) = row?;
        // payload stays a JSON string (double-encoded on the wire) so
        // every binding's existing parse path keeps working.
        out.push(json!({
            "id": id,
            "queue": q,
            "payload": payload,
            "worker_id": w,
            "attempts": attempts,
            "claim_expires_at": claim_expires_at,
        }));
    }
    Ok(Value::Array(out).to_string())
}

/// Batch ack. No SAVEPOINT, unlike the dead-letter paths: this DELETE
/// is the whole operation. `RETURNING id` is only counted, never
/// decoded into Rust, and nothing runs after it that could fail and
/// strand the deleted rows. The jobs are meant to be gone.
pub fn ack_batch(conn: &Connection, ids_json: &str, worker_id: &str) -> rusqlite::Result<i64> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM _honker_live
         WHERE id IN (SELECT value FROM json_each(?1))
           AND worker_id = ?2
           AND claim_expires_at >= unixepoch()
         RETURNING id",
    )?;
    let mut rows = stmt.query(rusqlite::params![ids_json, worker_id])?;
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
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
pub fn enqueue(
    conn: &Connection,
    queue: &str,
    payload: &str,
    run_at: Option<i64>,
    delay: Option<i64>,
    priority: i64,
    max_attempts: i64,
    expires: Option<i64>,
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
    Ok(id)
}

/// Single-job ack. DELETEs the row if the caller's claim is still
/// valid. Returns 1 on success, 0 if the claim expired or the row
/// isn't ours.
pub fn ack(conn: &Connection, job_id: i64, worker_id: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND worker_id = ?2 AND claim_expires_at >= unixepoch()",
        rusqlite::params![job_id, worker_id],
    )?;
    Ok(deleted as i64)
}

/// Retry or fail based on `attempts` vs `max_attempts`. If another
/// attempt is allowed, flips the row back to `'pending'` with
/// `run_at = unixepoch() + delay_s` and fires a wake. Otherwise
/// DELETEs from `_honker_live` and INSERTs into `_honker_dead`
/// with `last_error=error`.
///
/// Returns 1 if either branch ran, 0 if the claim is no longer valid
/// (expired / not our worker / row moved on).
pub fn retry(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
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
        .optional()?;
    let Some((id, queue, payload, priority, run_at, max_attempts, attempts, created_at)) = row
    else {
        return Ok(0);
    };
    if attempts >= max_attempts {
        // DELETE then INSERT as two statements: without a savepoint a
        // failing INSERT leaves the job in neither table. Measured on
        // this branch before the fix: live=0, dead=0.
        in_savepoint(conn, "honker_retry_dead_letter", || {
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
            Ok(())
        })?;
    } else {
        conn.execute(
            "UPDATE _honker_live
             SET state = 'pending',
                 run_at = unixepoch() + ?2,
                 worker_id = NULL,
                 claim_expires_at = NULL
             WHERE id = ?1",
            rusqlite::params![id, delay_s],
        )?;
        // Wake comes from the live-table UPDATE + commit (data_version).
        // No synthetic notification row — see enqueue() for rationale.
    }
    Ok(1)
}

/// Unconditionally move the claim to `_honker_dead` with the given
/// error. Returns 1 if moved, 0 if not our claim.
///
/// The DELETE uses RETURNING, so the row is already gone by the time
/// the row mapper decodes it. A decode failure there — a non-integer
/// `attempts`, say — would otherwise leave the job in neither
/// `_honker_live` nor `_honker_dead`: silent job loss. Verified: an
/// error out of the mapper alone does NOT undo the DELETE. So the
/// delete-decode-insert runs inside [`in_savepoint`], which rolls it
/// back before propagating.
pub fn fail(conn: &Connection, job_id: i64, worker_id: &str, error: &str) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_fail", || {
        fail_inner(conn, job_id, worker_id, error)
    })
}

fn fail_inner(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    error: &str,
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
        .optional()?;
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
pub fn cancel(conn: &Connection, job_id: i64) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "DELETE FROM _honker_live WHERE id = ?1 AND state IN ('pending', 'processing')",
        rusqlite::params![job_id],
    )?;
    Ok(n as i64)
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
        .optional()?;
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
///
/// Runs in a SAVEPOINT for the same reason as
/// `dead_letter_exhausted_claimable`: DELETE ... RETURNING, then decode,
/// then INSERT. Measured before the fix: a decode failure and a failing
/// dead-table INSERT both gave live=0, dead=0.
pub fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_sweep_expired", || {
        sweep_expired_inner(conn, queue)
    })
}

fn sweep_expired_inner(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
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
    for r in rows {
        insert.execute(rusqlite::params![r.0, r.1, r.2, r.3, r.4, r.5, r.6, r.7])?;
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
        .optional()?;
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
pub fn scheduler_tick(conn: &Connection, now_unix: i64) -> rusqlite::Result<String> {
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
            let job_id = enqueue(
                conn,
                &queue,
                &payload,
                None,
                None,
                priority,
                max_attempts,
                expires_s,
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
        .optional()?;
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

pub fn stream_publish(
    conn: &Connection,
    topic: &str,
    key: Option<&str>,
    payload: &str,
) -> rusqlite::Result<i64> {
    // Stream row INSERT advances data_version on commit — same wake
    // path as enqueue. No synthetic notification row (see enqueue).
    let offset: i64 = conn.query_row(
        "INSERT INTO _honker_stream (topic, key, payload)
         VALUES (?1, ?2, ?3)
         RETURNING offset",
        rusqlite::params![topic, key, payload],
        |r| r.get(0),
    )?;
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

// Five lookups used to end in `.ok()`, which throws away every error
// and not just QueryReturnedNoRows. A real SQLite error read as "no
// row" turns a broken database into a silent no-op. Each site gets two
// tests: a genuine miss must still be a miss, and a broken stored type
// must reach the caller.
//
// `attempts` is declared INTEGER but SQLite is dynamically typed, so
// writing non-numeric text into it sticks. That is the cheapest way to
// make the row mapper fail on a row that really exists.
#[cfg(test)]
mod optional_error_tests {
    use super::{
        claim_batch, fail, get_job, in_savepoint, lock_acquire, result_get, retry, sweep_expired,
        to_sql_err,
    };
    use crate::{attach_honker_functions, bootstrap_honker_schema};
    use rusqlite::Connection;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        conn
    }

    /// Enqueue one job on `emails` and claim it for `w1`.
    fn claimed_job(conn: &Connection) -> i64 {
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
        id
    }

    fn corrupt_attempts(conn: &Connection, id: i64) {
        conn.execute(
            "UPDATE _honker_live SET attempts = 'not-a-number' WHERE id = ?1",
            [id],
        )
        .unwrap();
    }

    fn live_count(conn: &Connection, id: i64) -> i64 {
        conn.query_row(
            "SELECT count(*) FROM _honker_live WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn dead_count(conn: &Connection, id: i64) -> i64 {
        conn.query_row(
            "SELECT count(*) FROM _honker_dead WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    // ---------------- retry ----------------

    #[test]
    fn retry_on_a_missing_job_is_still_a_miss() {
        let conn = db();
        assert_eq!(
            retry(&conn, 999_999, "w1", 10, "boom").unwrap(),
            0,
            "a job id that was never enqueued must return 0, not an error"
        );
    }

    #[test]
    fn retry_surfaces_a_decode_error() {
        let conn = db();
        let id = claimed_job(&conn);
        corrupt_attempts(&conn, id);
        let err = retry(&conn, id, "w1", 10, "boom")
            .expect_err("a non-integer attempts column must reach the caller, not read as a miss");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
    }

    // ---------------- fail ----------------

    #[test]
    fn fail_on_a_missing_job_is_still_a_miss() {
        let conn = db();
        assert_eq!(
            fail(&conn, 999_999, "w1", "boom").unwrap(),
            0,
            "a job id that was never enqueued must return 0, not an error"
        );
    }

    // The one that loses data. fail() DELETEs with RETURNING, so the row
    // is already gone when the mapper decodes it. Propagating the error
    // is not enough on its own: measured, the bare DELETE stays
    // committed and the job ends up in neither table. The SAVEPOINT in
    // fail() is what puts it back. Assert the row, not just the error.
    #[test]
    fn fail_surfaces_a_decode_error_and_rolls_the_delete_back() {
        let conn = db();
        let id = claimed_job(&conn);
        corrupt_attempts(&conn, id);

        let err = fail(&conn, id, "w1", "boom")
            .expect_err("a non-integer attempts column must reach the caller, not read as a miss");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the DELETE must roll back: the job has to stay in _honker_live, \
             not vanish from both tables"
        );
        assert_eq!(
            dead_count(&conn, id),
            0,
            "the job must not be half-moved into _honker_dead"
        );
    }

    // The savepoint must not disturb the path that works.
    #[test]
    fn fail_still_moves_a_healthy_job_to_dead() {
        let conn = db();
        let id = claimed_job(&conn);
        assert_eq!(fail(&conn, id, "w1", "boom").unwrap(), 1);
        assert_eq!(live_count(&conn, id), 0, "the live row must be gone");
        assert_eq!(dead_count(&conn, id), 1, "the dead row must be written");
        let last_error: String = conn
            .query_row(
                "SELECT last_error FROM _honker_dead WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(last_error, "boom");
    }

    // fail() opens a SAVEPOINT. Nesting inside a caller's transaction
    // has to keep working, and the rollback must undo only fail()'s own
    // work, not the caller's.
    #[test]
    fn fail_rolls_back_only_its_own_work_inside_an_outer_transaction() {
        let conn = db();
        let id = claimed_job(&conn);
        corrupt_attempts(&conn, id);

        conn.execute_batch("BEGIN").unwrap();
        conn.execute(
            "INSERT INTO _honker_results (job_id, value) VALUES (4242, 'caller-work')",
            [],
        )
        .unwrap();
        let err = fail(&conn, id, "w1", "boom").expect_err("the decode error must still propagate");
        assert!(err.to_string().contains("attempts"), "got: {err}");
        conn.execute_batch("COMMIT").unwrap();

        assert_eq!(
            live_count(&conn, id),
            1,
            "fail()'s DELETE must be rolled back inside an outer transaction too"
        );
        let caller_row: i64 = conn
            .query_row(
                "SELECT count(*) FROM _honker_results WHERE job_id = 4242",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            caller_row, 1,
            "ROLLBACK TO SAVEPOINT must not discard work the caller did before fail()"
        );
    }

    // ---------------- dead-letter savepoints ----------------
    //
    // Four operations do delete-then-more-work. Only `fail()` was
    // covered before; the other three were measured losing the job the
    // same way (issue #133 / PR #138 review):
    //
    //   claim_batch -> dead_letter_exhausted_claimable  live=0, dead=0
    //   retry()'s dead-letter branch                    live=0, dead=0
    //   sweep_expired                                   live=0, dead=0

    /// Make every INSERT into `_honker_dead` fail. Stands in for a
    /// constraint violation or an I/O error on the second half of a
    /// dead-letter move.
    fn block_dead_inserts(conn: &Connection) {
        conn.execute_batch(
            "CREATE TRIGGER test_block_dead BEFORE INSERT ON _honker_dead
             BEGIN SELECT RAISE(ABORT, 'dead insert blocked'); END",
        )
        .unwrap();
    }

    /// A pending, due job whose `attempts` cannot be decoded. Text
    /// compares greater than any integer in SQLite, so it also matches
    /// `attempts >= max_attempts` and lands in the dead-letter sweep.
    fn undecodable_pending_job(conn: &Connection) -> i64 {
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        corrupt_attempts(conn, id);
        id
    }

    /// A due job that has already used its whole attempt budget, so an
    /// ordinary claim dead-letters it.
    fn exhausted_pending_job(conn: &Connection) -> i64 {
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 1, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute("UPDATE _honker_live SET attempts = 1 WHERE id = ?1", [id])
            .unwrap();
        id
    }

    // The hot path: no fail() call involved, an ordinary claim triggers
    // it. Before the savepoint this returned the decode error with the
    // job gone from both tables.
    #[test]
    fn claim_batch_rolls_back_a_dead_letter_decode_failure() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        let err = claim_batch(&conn, "emails", "w1", 8, 300)
            .expect_err("a non-integer attempts column must reach the caller");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the dead-letter DELETE must roll back: the job has to stay in \
             _honker_live, not vanish from both tables"
        );
        assert_eq!(dead_count(&conn, id), 0, "and it must not be half-moved");
    }

    // The DELETE takes the whole matching set before anything is
    // decoded, so one bad row used to lose every job beside it.
    #[test]
    fn claim_batch_dead_letter_keeps_the_whole_batch() {
        let conn = db();
        let bad = undecodable_pending_job(&conn);
        let neighbour = undecodable_pending_job(&conn);
        let _ = claim_batch(&conn, "emails", "w1", 8, 300)
            .expect_err("a non-integer attempts column must reach the caller");
        assert_eq!(
            (live_count(&conn, bad), live_count(&conn, neighbour)),
            (1, 1),
            "one undecodable row must not take the rest of the batch with it"
        );
    }

    #[test]
    fn claim_batch_rolls_back_when_the_dead_insert_fails() {
        let conn = db();
        let id = exhausted_pending_job(&conn);
        block_dead_inserts(&conn);
        let err = claim_batch(&conn, "emails", "w1", 8, 300)
            .expect_err("a failing _honker_dead INSERT must reach the caller");
        assert!(
            err.to_string().contains("dead insert blocked"),
            "expected the trigger's message, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the DELETE must roll back when the dead-table INSERT fails"
        );
        assert_eq!(dead_count(&conn, id), 0);
    }

    // A genuine claim must be untouched by the savepoint: the exhausted
    // job still dead-letters, the healthy one is still handed out.
    #[test]
    fn claim_batch_still_dead_letters_and_claims_normally() {
        let conn = db();
        let exhausted = exhausted_pending_job(&conn);
        let fresh: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        let claimed = claim_batch(&conn, "emails", "w1", 8, 300).unwrap();
        assert_eq!(live_count(&conn, exhausted), 0);
        assert_eq!(dead_count(&conn, exhausted), 1);
        assert!(
            claimed.contains(&format!("\"id\":{fresh}")),
            "the healthy job must still be claimed, got: {claimed}"
        );
    }

    // retry()'s dead-letter branch: DELETE then INSERT as two
    // statements. Measured before the fix: live=0, dead=0.
    #[test]
    fn retry_rolls_back_the_dead_letter_when_the_insert_fails() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 1, NULL)",
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
        block_dead_inserts(&conn);
        let err = retry(&conn, id, "w1", 10, "boom")
            .expect_err("a failing _honker_dead INSERT must reach the caller");
        assert!(
            err.to_string().contains("dead insert blocked"),
            "expected the trigger's message, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "retry() must not destroy the job in a way fail() no longer can"
        );
        assert_eq!(dead_count(&conn, id), 0);
    }

    #[test]
    fn retry_still_dead_letters_an_exhausted_job() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 1, NULL)",
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
        assert_eq!(retry(&conn, id, "w1", 10, "boom").unwrap(), 1);
        assert_eq!(live_count(&conn, id), 0);
        assert_eq!(dead_count(&conn, id), 1);
    }

    // sweep_expired: same DELETE ... RETURNING then decode then INSERT.
    #[test]
    fn sweep_expired_rolls_back_a_decode_failure() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        conn.execute(
            "UPDATE _honker_live SET expires_at = unixepoch() - 10 WHERE id = ?1",
            [id],
        )
        .unwrap();
        let err = sweep_expired(&conn, "emails")
            .expect_err("a non-integer attempts column must reach the caller");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the sweep's DELETE must roll back, not lose the job"
        );
        assert_eq!(dead_count(&conn, id), 0);
    }

    #[test]
    fn sweep_expired_rolls_back_when_the_dead_insert_fails() {
        let conn = db();
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute(
            "UPDATE _honker_live SET expires_at = unixepoch() - 10 WHERE id = ?1",
            [id],
        )
        .unwrap();
        block_dead_inserts(&conn);
        let err = sweep_expired(&conn, "emails")
            .expect_err("a failing _honker_dead INSERT must reach the caller");
        assert!(
            err.to_string().contains("dead insert blocked"),
            "expected the trigger's message, got: {err}"
        );
        assert_eq!(live_count(&conn, id), 1);
        assert_eq!(dead_count(&conn, id), 0);
    }

    // Genuine misses must still be misses, not errors, at every site
    // that gained a savepoint.
    #[test]
    fn savepoint_sites_still_report_a_genuine_miss() {
        let conn = db();
        assert_eq!(
            sweep_expired(&conn, "emails").unwrap(),
            0,
            "nothing expired is 0, not an error"
        );
        assert_eq!(
            claim_batch(&conn, "emails", "w1", 8, 300).unwrap(),
            "[]",
            "an empty queue claims nothing, without erroring"
        );
        assert_eq!(
            retry(&conn, 999_999, "w1", 10, "boom").unwrap(),
            0,
            "a job id that was never enqueued must return 0"
        );
        assert_eq!(fail(&conn, 999_999, "w1", "boom").unwrap(), 0);
        assert!(
            conn.is_autocommit(),
            "no savepoint may be left on the stack"
        );
    }

    // ---------------- through the SQL functions ----------------
    //
    // `SELECT honker_*(...)` is the only path bindings and ORM users
    // take, and it is the one the savepoints have to survive: the outer
    // statement is still stepping while the scalar function runs
    // SAVEPOINT / ROLLBACK TO on the same connection. Nothing pinned
    // that before.

    #[test]
    fn sql_honker_fail_rolls_back_through_the_scalar_function() {
        let conn = db();
        let id = claimed_job(&conn);
        corrupt_attempts(&conn, id);
        let err = conn
            .query_row("SELECT honker_fail(?1, 'w1', 'boom')", [id], |r| {
                r.get::<_, i64>(0)
            })
            .expect_err("the decode error must surface through the SQL function too");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "SAVEPOINT rollback from inside a scalar function must still \
             put the job back"
        );
        assert_eq!(dead_count(&conn, id), 0);
        assert!(
            conn.is_autocommit(),
            "no transaction may be left open on the caller's connection"
        );
    }

    #[test]
    fn sql_honker_fail_still_works_on_the_healthy_path() {
        let conn = db();
        let id = claimed_job(&conn);
        let moved: i64 = conn
            .query_row("SELECT honker_fail(?1, 'w1', 'boom')", [id], |r| r.get(0))
            .unwrap();
        assert_eq!(moved, 1);
        assert_eq!(live_count(&conn, id), 0);
        assert_eq!(dead_count(&conn, id), 1);
        assert!(conn.is_autocommit());
    }

    #[test]
    fn sql_honker_claim_batch_rolls_back_the_dead_letter_sweep() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        let err = conn
            .query_row(
                "SELECT honker_claim_batch('emails', 'w1', 8, 300)",
                [],
                |r| r.get::<_, String>(0),
            )
            .expect_err("the decode error must surface through the SQL function too");
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the dead-letter sweep must roll back on the binding path too"
        );
        assert_eq!(dead_count(&conn, id), 0);
        assert!(conn.is_autocommit());
    }

    #[test]
    fn sql_honker_sweep_expired_rolls_back() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        conn.execute(
            "UPDATE _honker_live SET expires_at = unixepoch() - 10 WHERE id = ?1",
            [id],
        )
        .unwrap();
        let err = conn
            .query_row("SELECT honker_sweep_expired('emails')", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect_err("the decode error must surface through the SQL function too");
        assert!(err.to_string().contains("attempts"), "got: {err}");
        assert_eq!(live_count(&conn, id), 1);
        assert_eq!(dead_count(&conn, id), 0);
        assert!(conn.is_autocommit());
    }

    // ---------------- in_savepoint itself ----------------

    // Defect 4 from the review: the old code did
    // `let _ = conn.execute_batch("ROLLBACK TO SAVEPOINT ...")`, so a
    // failed rollback left the connection in an unknown state with no
    // signal. The rollback failure has to be reported *and* the original
    // cause has to survive, since callers match on its text.
    #[test]
    fn in_savepoint_reports_a_rollback_failure_without_losing_the_cause() {
        let conn = db();
        // An outer transaction, so in_savepoint nests and undoes with
        // ROLLBACK TO SAVEPOINT rather than a plain ROLLBACK.
        conn.execute_batch("BEGIN").unwrap();
        let err = in_savepoint(&conn, "honker_test_sp", || {
            // The frame is gone before the body fails, so the rollback
            // below cannot succeed.
            conn.execute_batch("RELEASE SAVEPOINT honker_test_sp")?;
            Err::<(), _>(to_sql_err("original cause"))
        })
        .expect_err("the body error must still reach the caller");
        let msg = err.to_string();
        assert!(
            msg.contains("original cause"),
            "the original error must not be lost, got: {msg}"
        );
        assert!(
            msg.contains("ROLLBACK TO SAVEPOINT honker_test_sp failed"),
            "a failed rollback must be reported, not discarded, got: {msg}"
        );
        conn.execute_batch("ROLLBACK").unwrap();
    }

    // Defect 5 from the review: RELEASE of the outermost savepoint is
    // the COMMIT. When it failed, fail() returned the error with the
    // transaction it opened still open — is_autocommit() == false, and
    // the connection's own reads claimed the job had been dead-lettered.
    // Bindings hold long-lived connections, so every later call would
    // join that transaction.
    //
    // Reproduced with a rollback-journal database and a second
    // connection holding a read transaction, which is the configuration
    // honker documents supporting for ORM users.
    #[test]
    fn fail_leaves_no_open_transaction_when_release_fails() {
        let dir = std::env::temp_dir().join(format!(
            "honker-release-fail-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("honker.db");
        let _ = std::fs::remove_file(&path);

        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch("PRAGMA journal_mode = delete; PRAGMA busy_timeout = 0;")
            .unwrap();
        attach_honker_functions(&writer).unwrap();
        bootstrap_honker_schema(&writer).unwrap();
        let id = claimed_job(&writer);

        // A concurrent read transaction blocks the writer's commit.
        let reader = Connection::open(&path).unwrap();
        reader
            .execute_batch("PRAGMA busy_timeout = 0; BEGIN;")
            .unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap();

        let err = fail(&writer, id, "w1", "boom")
            .expect_err("the blocked commit must reach the caller as an error");
        assert!(
            err.to_string().contains("locked") || err.to_string().contains("busy"),
            "expected a lock conflict, got: {err}"
        );
        assert!(
            writer.is_autocommit(),
            "a failed RELEASE must not leave the caller inside the transaction \
             fail() opened; the connection is long-lived and every later call \
             would silently join it"
        );
        assert_eq!(
            live_count(&writer, id),
            1,
            "the job must not read as dead-lettered after a commit that failed"
        );
        assert_eq!(dead_count(&writer, id), 0);

        drop(reader);
        drop(writer);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---------------- get_job ----------------

    #[test]
    fn get_job_on_a_missing_job_is_still_a_miss() {
        let conn = db();
        assert_eq!(
            get_job(&conn, 999_999).unwrap(),
            "",
            "a job id that was never enqueued must return the empty string"
        );
    }

    #[test]
    fn get_job_surfaces_a_decode_error() {
        let conn = db();
        let id = claimed_job(&conn);
        corrupt_attempts(&conn, id);
        let err = get_job(&conn, id).expect_err(
            "a non-integer attempts column must reach the caller, not read as a missing job",
        );
        assert!(
            err.to_string().contains("attempts"),
            "expected the error to name the bad column, got: {err}"
        );
    }

    // ---------------- lock_acquire ----------------

    #[test]
    fn lock_acquire_grants_then_refuses_without_erroring() {
        let conn = db();
        assert_eq!(
            lock_acquire(&conn, "leader", "a", 300).unwrap(),
            1,
            "an unheld lock must be granted"
        );
        assert_eq!(
            lock_acquire(&conn, "leader", "b", 300).unwrap(),
            0,
            "a lock held by someone else must return 0, not an error"
        );
    }

    // `owner` is declared TEXT, but a BLOB keeps its type under TEXT
    // affinity, so this is a row that exists and cannot be decoded as a
    // String. Under `.ok()` this returned 0 — indistinguishable from
    // "another process holds the lock", so no leader could ever start.
    #[test]
    fn lock_acquire_surfaces_a_decode_error() {
        let conn = db();
        conn.execute(
            "INSERT INTO _honker_locks (name, owner, expires_at)
             VALUES ('leader', x'ff', unixepoch() + 300)",
            [],
        )
        .unwrap();
        let err = lock_acquire(&conn, "leader", "a", 300)
            .expect_err("an undecodable owner must reach the caller, not read as 'not ours'");
        assert!(
            err.to_string().contains("owner"),
            "expected the error to name the bad column, got: {err}"
        );
    }

    // ---------------- result_get ----------------

    #[test]
    fn result_get_on_a_missing_result_is_still_a_miss() {
        let conn = db();
        assert_eq!(
            result_get(&conn, 999_999).unwrap(),
            None,
            "a job with no stored result must return None, not an error"
        );
    }

    #[test]
    fn result_get_surfaces_a_decode_error() {
        let conn = db();
        conn.execute(
            "INSERT INTO _honker_results (job_id, value, expires_at)
             VALUES (7, 'ok', 'not-a-timestamp')",
            [],
        )
        .unwrap();
        let err = result_get(&conn, 7)
            .expect_err("a non-integer expires_at must reach the caller, not read as a miss");
        assert!(
            err.to_string().contains("expires_at"),
            "expected the error to name the bad column, got: {err}"
        );
    }
}
