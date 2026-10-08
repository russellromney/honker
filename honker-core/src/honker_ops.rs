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
/// `name` is the public SQL function that needs the savepoint, such as
/// `honker_claim_batch`. When SQLite refuses the savepoint because a
/// write statement is still active, the error tells the caller how to
/// call that function instead.
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
///   * A panicking `body` unwinds past every one of those paths, so the
///     undo also hangs off a drop guard. See [`UnwindUndo`].
pub(crate) fn in_savepoint<T>(
    conn: &Connection,
    name: &str,
    body: impl FnOnce() -> rusqlite::Result<T>,
) -> rusqlite::Result<T> {
    // True means this SAVEPOINT is what opens the transaction, so we
    // own it and nobody else's work is inside it.
    let owns_transaction = conn.is_autocommit();
    conn.execute_batch(&format!("SAVEPOINT {name}"))
        .map_err(|err| {
            // SQLite cannot create a savepoint while a write statement is active.
            // Keep SQLite's message and add how to call the operation instead.
            // `name` is the public SQL function the caller used.
            match err {
                rusqlite::Error::SqliteFailure(code, Some(message))
                    if code.code == rusqlite::ErrorCode::DatabaseBusy
                        && message == "cannot open savepoint - SQL statements in progress" =>
                {
                    rusqlite::Error::SqliteFailure(
                        code,
                        Some(format!(
                            "{message}; honker: {name} requires a separate SELECT after you \
                     finish all write/RETURNING cursors; do not call it from a trigger \
                     or write statement. An explicit surrounding transaction is supported"
                        )),
                    )
                }
                other => other,
            }
        })?;
    let mut guard = UnwindUndo {
        conn,
        name,
        owns_transaction,
        armed: true,
    };
    let result = match body() {
        Ok(value) => match conn.execute_batch(&format!("RELEASE SAVEPOINT {name}")) {
            Ok(()) => Ok(value),
            // RELEASE of the outermost savepoint is the COMMIT. If it
            // fails (a lock conflict on a rollback-journal database,
            // say) the write is not durable, so roll it back instead of
            // leaving it dangling.
            Err(release_err) => Err(undo_savepoint(conn, name, owns_transaction, release_err)),
        },
        Err(body_err) => Err(undo_savepoint(conn, name, owns_transaction, body_err)),
    };
    // Every path above already left the connection in a known state, so
    // the guard has nothing left to do.
    guard.armed = false;
    result
}

/// Undoes [`in_savepoint`]'s frame when `body` unwinds instead of
/// returning `Err`. Disarmed on every ordinary path.
///
/// Without it a panic skips the undo entirely. Measured: the DELETE
/// stayed applied, `is_autocommit()` was left false, and the next
/// honker call on that connection silently joined the leaked
/// transaction and returned `Ok(0)` — a miss, not an error. rusqlite
/// catches a panic raised inside a scalar function and hands the caller
/// an "unwinding panic" error, so the connection survives to be reused:
/// through `SELECT honker_*(...)` the frame was still on the stack
/// afterwards, on the long-lived connection every binding holds.
struct UnwindUndo<'a> {
    conn: &'a Connection,
    name: &'a str,
    owns_transaction: bool,
    armed: bool,
}

impl Drop for UnwindUndo<'_> {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        // Only reached while unwinding, so there is no error to return
        // and no second panic to raise. Undo the frame the same way the
        // error path does; if that fails, the connection really is in an
        // unknown state and stderr is the only channel left.
        let failures = undo_savepoint_frame(self.conn, self.name, self.owns_transaction);
        if !failures.is_empty() {
            eprintln!(
                "honker: error: undoing SAVEPOINT {} after a panic left the \
                 connection in an unknown state: {}",
                self.name,
                failures.join("; ")
            );
        }
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
    let failures = undo_savepoint_frame(conn, name, owns_transaction);
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

/// Roll the frame back and pop it. Returns what went wrong while doing
/// so; empty means the connection is back in a known state.
fn undo_savepoint_frame(conn: &Connection, name: &str, owns_transaction: bool) -> Vec<String> {
    let mut failures: Vec<String> = Vec::new();

    if conn.is_autocommit() {
        // SQLite already rolled the whole transaction back on its own —
        // an ABORT, SQLITE_FULL or an I/O error does that. There is no
        // savepoint left to roll back to, and issuing one would turn a
        // clean state into a bogus second error.
        return failures;
    }

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

    failures
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
    // job_id}` to the output array. Atomic on its own (#173): ticks
    // from any number of connections enqueue each boundary once. A
    // scheduler leader still holds the `_honker_locks` entry
    // 'honker-scheduler' so only one process runs the loop.
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

    // Fenced forms. The extra last argument is the `attempts` value the
    // claim returned for this job: the claim's token. The guard is
    // `id + worker_id + attempts = token + state = 'processing'`, with
    // no lease check. A reclaim bumps `attempts` and dead-letter,
    // expiry and cancel remove the row, so a stale handler (even one
    // with the same worker id) matches nothing and gets 0, while a
    // late handler whose job nobody reclaimed still completes. The
    // shorter forms above are unfenced: they check worker_id and the
    // lease only. See issue #176.
    //
    // honker_ack(job_id, worker_id, attempt) -> 1 if ack'd, 0 if this
    // attempt no longer owns the job.
    conn.create_scalar_function("honker_ack", 3, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let attempt: i64 = arg_i64(ctx, 2)?;
        let db = unsafe { ctx.get_connection() }?;
        ack_fenced(&db, job_id, &worker_id, attempt).map_err(to_sql_err)
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

    // honker_retry(job_id, worker_id, delay_s, error, attempt): fenced
    // form of the above. See honker_ack(job_id, worker_id, attempt).
    conn.create_scalar_function("honker_retry", 5, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let delay_s: i64 = arg_i64(ctx, 2)?;
        let error: String = ctx.get(3)?;
        let attempt: i64 = arg_i64(ctx, 4)?;
        let db = unsafe { ctx.get_connection() }?;
        retry_fenced(&db, job_id, &worker_id, delay_s, &error, attempt).map_err(to_sql_err)
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

    // honker_fail(job_id, worker_id, error, attempt): fenced form.
    conn.create_scalar_function("honker_fail", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let error: String = ctx.get(2)?;
        let attempt: i64 = arg_i64(ctx, 3)?;
        let db = unsafe { ctx.get_connection() }?;
        fail_fenced(&db, job_id, &worker_id, &error, attempt).map_err(to_sql_err)
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

    // honker_heartbeat(job_id, worker_id, extend_s, attempt): fenced
    // form. Sets claim_expires_at = now + extend_s even if the lease
    // already lapsed, as long as nobody reclaimed the job.
    conn.create_scalar_function("honker_heartbeat", 4, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let worker_id: String = ctx.get(1)?;
        let extend_s: i64 = arg_i64(ctx, 2)?;
        let attempt: i64 = arg_i64(ctx, 3)?;
        let db = unsafe { ctx.get_connection() }?;
        heartbeat_fenced(&db, job_id, &worker_id, extend_s, attempt).map_err(to_sql_err)
    })?;

    // honker_cancel(job_id) -> 1 if a pending/processing row was removed,
    // 0 otherwise. Idempotent on missing.
    conn.create_scalar_function("honker_cancel", 1, FunctionFlags::SQLITE_UTF8, |ctx| {
        let job_id: i64 = arg_i64(ctx, 0)?;
        let db = unsafe { ctx.get_connection() }?;
        cancel(&db, job_id).map_err(to_sql_err)
    })?;

    // honker_cancel(queue, job_id) -> 1 if a pending/processing row in
    // THAT queue was removed, 0 otherwise. Registered under the same
    // name at a different arity: SQLite dispatches on (name, nArg), so
    // this queue-scoped form and the global 1-arg form above coexist on
    // one connection. Bindings move to this form per-package without a
    // lockstep release; see `has_queue_scoped_cancel` for the
    // connect-time probe that tells them which forms are loaded.
    conn.create_scalar_function("honker_cancel", 2, FunctionFlags::SQLITE_UTF8, |ctx| {
        let queue: String = ctx.get(0)?;
        let job_id: i64 = arg_i64(ctx, 1)?;
        let db = unsafe { ctx.get_connection() }?;
        cancel_in_queue(&db, &queue, job_id).map_err(to_sql_err)
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

/// Most rows one housekeeping step of [`claim_batch`] touches.
///
/// Promotion, expiry and the exhausted-lease sweep each run inside the
/// claim's write transaction. A backlog whose deadlines all pass at
/// once (50k jobs scheduled for the same second, say) would otherwise
/// be moved in one claim and stall every writer behind it. Anything
/// left over is moved by the next claims; until then the claim's own
/// filters keep it from being handed out wrongly.
pub const CLAIM_HOUSEKEEPING_LIMIT: i64 = 1000;

/// `scheduled → pending` for rows whose `run_at` has passed. Uses
/// `_honker_live_scheduled (queue, run_at)`, so it only touches rows
/// whose deadline passed.
const PROMOTE_SQL: &str = "UPDATE _honker_live SET state = 'pending'
     WHERE id IN (
       SELECT id FROM _honker_live
       WHERE queue = ?1 AND state = 'scheduled' AND run_at <= ?2
       ORDER BY run_at, id
       LIMIT ?3
     )";

/// Expired rows that nobody holds a valid lease on. A `processing` row
/// whose lease lapsed is included: its holder is gone or late, and no
/// claim can take it any more because it expired. Leaving it was
/// issue #177 — it stayed `processing` forever. A `processing` row with
/// a valid lease is left for its owner to finish. Uses
/// `_honker_live_expiry (queue, expires_at)`.
///
/// `?3` is the row limit; [`sweep_expired`] passes -1 (no limit).
const EXPIRE_IDS: &str = "SELECT id FROM _honker_live
       WHERE queue = ?1 AND expires_at <= ?2
         AND (state IN ('pending', 'scheduled')
              OR (state = 'processing' AND claim_expires_at < ?2))
       ORDER BY expires_at, id
       LIMIT ?3";

/// Lapsed leases that already used their attempt budget. Without this
/// a worker that dies on the last allowed attempt leaves a row nobody
/// may claim and nobody removes. Uses `_honker_live_processing_deadline
/// (queue, claim_expires_at)`, so it only touches lapsed leases.
///
/// `pending` and `scheduled` rows cannot be exhausted: enqueue rejects
/// `max_attempts < 1`, retry only re-queues while `attempts <
/// max_attempts`, and bootstrap moves exhausted rows written by older
/// builds when it migrates.
const EXHAUSTED_IDS: &str = "SELECT id FROM _honker_live
       WHERE queue = ?1 AND state = 'processing' AND claim_expires_at < ?2
         AND attempts >= max_attempts
       ORDER BY claim_expires_at, id
       LIMIT ?3";

/// The claim itself. Two arms:
///
/// * due `pending` rows, read in claim order straight off
///   `_honker_live_ready (queue, priority DESC, run_at, id)`, which
///   stops after `n` rows. `run_at <= now` keeps a future `pending` row
///   written by an older build (or by raw SQL) from running early; with
///   `INDEXED BY` it is a filter on the walk, not a different plan.
/// * `processing` rows whose lease lapsed, via
///   `_honker_live_processing_deadline`. Lapsed leases stay
///   `processing` until a claim takes them, so the late holder's fenced
///   ack still works until then.
///
/// SQLite does not allow ORDER BY/LIMIT on an arm of a compound
/// SELECT, so the first arm is wrapped in its own subquery.
const CLAIM_SQL: &str = "UPDATE _honker_live
     SET state = 'processing',
         worker_id = ?1,
         claim_expires_at = ?5 + ?4,
         claimed_at = ?5,
         attempts = attempts + 1
     WHERE id IN (
       SELECT id FROM (
         SELECT id, priority, run_at FROM (
           SELECT id, priority, run_at
           FROM _honker_live INDEXED BY _honker_live_ready
           WHERE queue = ?2 AND state = 'pending' AND run_at <= ?5
             AND attempts < max_attempts
             AND (expires_at IS NULL OR expires_at > ?5)
           ORDER BY priority DESC, run_at, id
           LIMIT ?3
         )
         UNION ALL
         SELECT id, priority, run_at FROM _honker_live
         WHERE queue = ?2 AND state = 'processing' AND claim_expires_at < ?5
           AND attempts < max_attempts
           AND (expires_at IS NULL OR expires_at > ?5)
       )
       ORDER BY priority DESC, run_at, id
       LIMIT ?3
     )
     RETURNING id, queue, payload, worker_id, attempts, claim_expires_at,
               claimed_at";

/// Move the rows `ids_sql` selects from `_honker_live` to
/// `_honker_dead` with `last_error = error`. Returns how many moved.
///
/// Two set statements, no per-row round trip: `INSERT INTO _honker_dead
/// SELECT ... WHERE id IN (ids)`, then `DELETE ... WHERE id IN (ids)`.
/// They select the same rows because both bind the same `now` (`?2`),
/// the order is total (`..., id`), and nothing else writes in between:
/// the caller's savepoint holds the write lock. With `unixepoch()` in
/// each statement instead, a row whose deadline fell between the two
/// would be deleted without being copied — a lost job. The row counts
/// are compared anyway, and a mismatch is an error, so the savepoint
/// rolls both back instead of losing or duplicating a job.
///
/// Must run inside a savepoint (issue #133).
fn move_to_dead(
    conn: &Connection,
    ids_sql: &str,
    queue: &str,
    now: i64,
    limit: i64,
    error: &str,
) -> rusqlite::Result<i64> {
    let copied = conn
        .prepare_cached(&format!(
            "INSERT INTO _honker_dead
               (id, queue, payload, priority, run_at, max_attempts,
                attempts, last_error, created_at, died_at)
             SELECT id, queue, payload, priority, run_at, max_attempts,
                    attempts, ?4, created_at, ?2
               FROM _honker_live WHERE id IN ({ids_sql})"
        ))?
        .execute(rusqlite::params![queue, now, limit, error])?;
    if copied == 0 {
        return Ok(0);
    }
    let deleted = conn
        .prepare_cached(&format!("DELETE FROM _honker_live WHERE id IN ({ids_sql})"))?
        .execute(rusqlite::params![queue, now, limit])?;
    if deleted != copied {
        return Err(to_sql_err(format!(
            "honker: moving rows to _honker_dead copied {copied} but deleted \
             {deleted}; rolled back"
        )));
    }
    Ok(copied as i64)
}

/// Returns JSON text: `[{"id":1,"queue":"...","payload":"...","worker_id":"...","attempts":N,"claim_expires_at":T,"claimed_at":T}, ...]`
///
/// One savepoint, one clock reading. `now` is read once and bound into
/// every statement: `unixepoch()` is stable inside one statement but
/// may tick between statements, and steps that disagree on the time
/// would disagree on which rows they mean. In order:
///
/// 1. promote due `scheduled` rows to `pending`
/// 2. move expired rows nobody holds a valid lease on to `_honker_dead`
///    (`'expired'`)
/// 3. move lapsed leases with no attempts left to `_honker_dead`
///    (`'max attempts exceeded'`)
/// 4. claim up to `n` rows: due `pending` rows and lapsed leases,
///    ordered by `priority DESC, run_at, id`
///
/// Steps 1–3 each touch at most [`CLAIM_HOUSEKEEPING_LIMIT`] rows, and
/// each reads an index that holds only rows whose deadline passed, so a
/// claim costs the same with 1k or 50k jobs waiting. The first
/// statement is a write, so the savepoint takes the write lock on a
/// fresh snapshot (see [`retry`]).
///
/// `claimed_at` is set to `now` on every successful claim, reclaim
/// included, so it measures the current attempt and not the first
/// one. `heartbeat()` deliberately leaves it alone — that is the whole
/// reason `claim_expires_at` cannot answer "how long has this been
/// running".
///
/// Here it is always a number: the UPDATE just wrote it on every row
/// this RETURNING sees. `get_job` is the one that can report
/// `"claimed_at": null`, for a job that has never been claimed.
///
/// Validity window: `claimed_at` is the start of the CURRENT claim and
/// is only meaningful while `claim_expires_at >= unixepoch()`. When a
/// claim lapses, `worker_id`, `claim_expires_at` and `claimed_at` stay
/// on the row and go stale together until the next claim takes it,
/// moves it to `_honker_dead`, or its holder's fenced call completes it.
pub fn claim_batch(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    in_savepoint(conn, "honker_claim_batch", || {
        claim_batch_inner(conn, queue, worker_id, n, timeout_s)
    })
}

fn claim_batch_inner(
    conn: &Connection,
    queue: &str,
    worker_id: &str,
    n: i64,
    timeout_s: i64,
) -> rusqlite::Result<String> {
    let now = now_unix(conn)?;
    let k = CLAIM_HOUSEKEEPING_LIMIT;
    conn.prepare_cached(PROMOTE_SQL)?
        .execute(rusqlite::params![queue, now, k])?;
    move_to_dead(conn, EXPIRE_IDS, queue, now, k, "expired")?;
    move_to_dead(conn, EXHAUSTED_IDS, queue, now, k, "max attempts exceeded")?;

    let mut stmt = conn.prepare_cached(CLAIM_SQL)?;
    let rows = stmt.query_map(
        rusqlite::params![worker_id, queue, n, timeout_s, now],
        |row| {
            Ok((
                row.get::<_, i64>(0)?,
                row.get::<_, String>(1)?,
                row.get::<_, String>(2)?,
                row.get::<_, String>(3)?,
                row.get::<_, i64>(4)?,
                row.get::<_, i64>(5)?,
                row.get::<_, i64>(6)?,
            ))
        },
    )?;
    let mut out = Vec::new();
    for row in rows {
        let (id, q, payload, w, attempts, claim_expires_at, claimed_at) = row?;
        // payload stays a JSON string (double-encoded on the wire) so
        // every binding's existing parse path keeps working.
        out.push(json!({
            "id": id,
            "queue": q,
            "payload": payload,
            "worker_id": w,
            "attempts": attempts,
            "claim_expires_at": claim_expires_at,
            "claimed_at": claimed_at,
        }));
    }
    Ok(Value::Array(out).to_string())
}

/// Batch ack. No SAVEPOINT, unlike the dead-letter paths: this DELETE
/// is the whole operation. `RETURNING id` is only counted, never
/// decoded into Rust, and nothing runs after it that could fail and
/// strand the deleted rows. The jobs are meant to be gone.
///
/// Each element of `ids_json` is either a plain id (unfenced: worker_id
/// and an unexpired lease, as before) or an `[id, attempt]` pair
/// (fenced: worker_id, `attempts = attempt` and `state = 'processing'`,
/// no lease check; see [`ack_fenced`]). Both kinds may be mixed in one
/// call; each element is judged by its own form. A pair is a JSON
/// array, so it never matches the plain-id branch.
pub fn ack_batch(conn: &Connection, ids_json: &str, worker_id: &str) -> rusqlite::Result<i64> {
    let mut stmt = conn.prepare_cached(
        "DELETE FROM _honker_live
         WHERE id IN (SELECT CASE WHEN type = 'array'
                                  THEN json_extract(value, '$[0]')
                                  ELSE value END
                      FROM json_each(?1))
           AND worker_id = ?2
           AND (
             (claim_expires_at >= unixepoch()
              AND id IN (SELECT value FROM json_each(?1) WHERE type <> 'array'))
             OR
             (state = 'processing'
              AND EXISTS (SELECT 1 FROM json_each(?1)
                          WHERE type = 'array'
                            AND json_extract(value, '$[0]') = _honker_live.id
                            AND json_extract(value, '$[1]') = _honker_live.attempts))
           )
         RETURNING id",
    )?;
    let mut rows = stmt.query(rusqlite::params![ids_json, worker_id])?;
    let mut count = 0;
    while rows.next()?.is_some() {
        count += 1;
    }
    Ok(count)
}

/// Return when `claim_batch()` could next return a job for this queue:
///
///   * the current time, if a due `pending` row is claimable now
///   * otherwise the earliest of a `scheduled` row's `run_at` and one
///     second after a held lease's `claim_expires_at` (a reclaim needs
///     `claim_expires_at < now`)
///
/// Rows with no attempts left, and rows that expire first, are ignored:
/// the next claim moves them to `_honker_dead` and never returns them.
/// An expiry deadline never makes a claim return a job, so it is not a
/// wake-up time here. A `scheduled` row whose `run_at` already passed
/// (no claim has promoted it yet) reports the current time, never a
/// time in the past.
///
/// Returns 0 if there is nothing to wait for.
pub fn queue_next_claim_at(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    let now = now_unix(conn)?;
    // Each deadline is decoded on its own, so a row with a non-integer
    // timestamp is an error (#166), not silently skipped.
    let (due, scheduled, lease): (bool, Option<i64>, Option<i64>) = conn.query_row(
        "SELECT
           EXISTS (
             SELECT 1 FROM _honker_live
             WHERE queue = ?1 AND state = 'pending' AND run_at <= ?2
               AND attempts < max_attempts
               AND (expires_at IS NULL OR expires_at > ?2)
           ),
           (SELECT run_at FROM _honker_live
             WHERE queue = ?1 AND state = 'scheduled'
               AND attempts < max_attempts
               AND (expires_at IS NULL OR expires_at > ?2)
             ORDER BY run_at LIMIT 1),
           (SELECT claim_expires_at + 1 FROM _honker_live
             WHERE queue = ?1 AND state = 'processing'
               AND claim_expires_at >= ?2
               AND attempts < max_attempts
               AND (expires_at IS NULL OR expires_at > ?2)
             ORDER BY claim_expires_at LIMIT 1)",
        rusqlite::params![queue, now],
        |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
    )?;
    if due {
        return Ok(now);
    }
    Ok(match (scheduled, lease) {
        (None, None) => 0,
        (a, b) => a.into_iter().chain(b).min().unwrap_or(0).max(now),
    })
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
///
/// State: `'scheduled'` when `run_at` is in the future, else
/// `'pending'`. A claim promotes a scheduled row once its `run_at`
/// passes. Keeping future rows out of `'pending'` keeps them out of the
/// ready index, so they cost a claim nothing.
///
/// `max_attempts` must be at least 1. A job with no attempts can never
/// be claimed, so it is rejected here instead of sitting in the queue.
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
    check_max_attempts("honker_enqueue", max_attempts)?;
    let now = now_unix(conn)?;
    enqueue_at(
        conn,
        now,
        queue,
        payload,
        run_at,
        delay,
        priority,
        max_attempts,
        expires,
    )
}

/// [`enqueue`] with the clock already read. `now` stands in for
/// `unixepoch()` in every rule above, so a caller that enqueues several
/// jobs in one operation ([`scheduler_tick`]) gives them all one time.
/// The caller checks `max_attempts`.
#[allow(clippy::too_many_arguments)]
fn enqueue_at(
    conn: &Connection,
    now: i64,
    queue: &str,
    payload: &str,
    run_at: Option<i64>,
    delay: Option<i64>,
    priority: i64,
    max_attempts: i64,
    expires: Option<i64>,
) -> rusqlite::Result<i64> {
    super::validate_json_payload(payload)?;
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
           (queue, payload, run_at, priority, max_attempts, expires_at, state)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6,
                 CASE WHEN ?3 > ?7 THEN 'scheduled' ELSE 'pending' END)
         RETURNING id",
        rusqlite::params![
            queue,
            payload,
            run_at_val,
            priority,
            max_attempts,
            expires_at,
            now
        ],
        |r| r.get(0),
    )?;
    Ok(id)
}

/// Single-job ack. DELETEs the row if the caller's claim is still
/// valid. Returns 1 on success, 0 if the claim expired or the row
/// isn't ours.
///
/// Unfenced: a stale handler with the same worker id as the current
/// holder passes this guard. Use [`ack_fenced`] with the claimed
/// `attempts`.
pub fn ack(conn: &Connection, job_id: i64, worker_id: &str) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND worker_id = ?2 AND claim_expires_at >= unixepoch()",
        rusqlite::params![job_id, worker_id],
    )?;
    Ok(deleted as i64)
}

/// Fenced single-job ack. `attempt` is the `attempts` value the claim
/// returned: the claim's token. DELETEs the row only if it is still
/// this claim: `id`, `worker_id`, `attempts = attempt` and
/// `state = 'processing'`. Returns 1 on success, 0 otherwise.
///
/// No lease check. A reclaim bumps `attempts`, and dead-letter, expiry
/// and cancel remove the row, so the token alone tells a stale call
/// from a late one. A late ack whose job nobody reclaimed succeeds and
/// the job does not run again; a stale ack after a reclaim, even by the
/// same worker id, matches nothing (issue #176).
pub fn ack_fenced(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    attempt: i64,
) -> rusqlite::Result<i64> {
    let deleted = conn.execute(
        "DELETE FROM _honker_live
         WHERE id = ?1 AND worker_id = ?2 AND attempts = ?3
           AND state = 'processing'",
        rusqlite::params![job_id, worker_id, attempt],
    )?;
    Ok(deleted as i64)
}

/// Retry or fail based on `attempts` vs `max_attempts`. If another
/// attempt is allowed, puts the row back with `run_at = unixepoch() +
/// delay_s`: `'scheduled'` when that is in the future (`delay_s > 0`),
/// else `'pending'`, and fires a wake. Otherwise
/// DELETEs from `_honker_live` and INSERTs into `_honker_dead`
/// with `last_error=error`.
///
/// Returns 1 if either branch ran, 0 if the claim is no longer valid
/// (expired / not our worker / row moved on).
///
/// Every state change is a single guarded write. There is no ownership
/// read before it. A read first would pin a WAL snapshot, and a commit
/// by any other connection before the write then fails the write with
/// SQLITE_BUSY_SNAPSHOT, which `busy_timeout` does not retry. A write
/// as the first statement takes the write lock on a fresh snapshot and
/// waits on the busy handler like any other write. The guards
/// (worker, state, unexpired lease, attempts) are checked at write
/// time, so a job another connection cancelled or reclaimed matches 0
/// rows and the call returns 0.
///
/// Unfenced: worker_id plus the lease, so a stale handler sharing the
/// new holder's worker id passes. See [`retry_fenced`].
pub fn retry(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
) -> rusqlite::Result<i64> {
    retry_with(conn, job_id, worker_id, delay_s, error, None)
}

/// Fenced [`retry`]. The guard is `id`, `worker_id`, `attempts =
/// attempt` and `state = 'processing'`, with no lease check; see
/// [`ack_fenced`]. Same write-first shape: the pending branch is one
/// guarded UPDATE (plus `attempts < max_attempts`), and the dead branch
/// is a guarded `DELETE ... RETURNING` plus the `_honker_dead` insert
/// inside one savepoint.
pub fn retry_fenced(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
    attempt: i64,
) -> rusqlite::Result<i64> {
    retry_with(conn, job_id, worker_id, delay_s, error, Some(attempt))
}

// The guards, unfenced then fenced. `?1` = id, `?2` = worker_id and,
// fenced only, `?3` = the claim's attempts token. Shared by the
// pending UPDATE, the branch read and the dead-letter DELETE so they
// can never disagree.
const RETRY_PENDING_SQL: [&str; 2] = [
    "UPDATE _honker_live
     SET state = CASE WHEN ?4 > 0 THEN 'scheduled' ELSE 'pending' END,
         run_at = unixepoch() + ?4,
         worker_id = NULL,
         claim_expires_at = NULL,
         claimed_at = NULL
     WHERE id = ?1 AND worker_id = ?2 AND state = 'processing'
       AND claim_expires_at >= unixepoch()
       AND attempts < max_attempts",
    "UPDATE _honker_live
     SET state = CASE WHEN ?4 > 0 THEN 'scheduled' ELSE 'pending' END,
         run_at = unixepoch() + ?4,
         worker_id = NULL,
         claim_expires_at = NULL,
         claimed_at = NULL
     WHERE id = ?1 AND worker_id = ?2 AND attempts = ?3
       AND state = 'processing'
       AND attempts < max_attempts",
];
const RETRY_EXHAUSTED_SQL: [&str; 2] = [
    "SELECT 1 FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2 AND state = 'processing'
       AND claim_expires_at >= unixepoch()
       AND attempts >= max_attempts",
    "SELECT 1 FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2 AND attempts = ?3
       AND state = 'processing'
       AND attempts >= max_attempts",
];
const RETRY_DEAD_SQL: [&str; 2] = [
    "DELETE FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2 AND state = 'processing'
       AND claim_expires_at >= unixepoch()
       AND attempts >= max_attempts
     RETURNING id, queue, payload, priority, run_at, max_attempts,
               attempts, created_at",
    "DELETE FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2 AND attempts = ?3
       AND state = 'processing'
       AND attempts >= max_attempts
     RETURNING id, queue, payload, priority, run_at, max_attempts,
               attempts, created_at",
];

/// Bind `?1` id, `?2` worker_id and, for the fenced text, `?3` the
/// attempt token. Extra trailing parameters start at `?4`.
fn bind_guard(
    stmt: &mut rusqlite::Statement<'_>,
    job_id: i64,
    worker_id: &str,
    attempt: Option<i64>,
) -> rusqlite::Result<()> {
    stmt.raw_bind_parameter(1, job_id)?;
    stmt.raw_bind_parameter(2, worker_id)?;
    if let Some(a) = attempt {
        stmt.raw_bind_parameter(3, a)?;
    }
    Ok(())
}

fn retry_with(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    delay_s: i64,
    error: &str,
    attempt: Option<i64>,
) -> rusqlite::Result<i64> {
    let form = usize::from(attempt.is_some());
    // Pending branch. One statement, so it needs no savepoint and works
    // wherever a plain UPDATE from a scalar function does.
    let updated = {
        let mut stmt = conn.prepare_cached(RETRY_PENDING_SQL[form])?;
        bind_guard(&mut stmt, job_id, worker_id, attempt)?;
        stmt.raw_bind_parameter(4, delay_s)?;
        stmt.raw_execute()?
    };
    if updated > 0 {
        // Wake comes from the live-table UPDATE + commit (data_version).
        // No synthetic notification row — see enqueue() for rationale.
        return Ok(1);
    }
    // Open the dead-letter savepoint only when there may be something to
    // move. A miss stays savepoint-free, as it was before. This read only
    // chooses the path: the DELETE below rechecks every guard, and the
    // UPDATE above already ran, so the read cannot pin an older snapshot
    // ahead of the first write.
    let exhausted = {
        let mut stmt = conn.prepare_cached(RETRY_EXHAUSTED_SQL[form])?;
        bind_guard(&mut stmt, job_id, worker_id, attempt)?;
        stmt.raw_query().next()?.is_some()
    };
    if !exhausted {
        return Ok(0);
    }
    // Exhausted branch: same shape as fail(). The DELETE ... RETURNING and
    // the INSERT run in one savepoint, so a failure in the second half
    // cannot lose the job.
    in_savepoint(conn, "honker_retry", || {
        let row = {
            let mut stmt = conn.prepare_cached(RETRY_DEAD_SQL[form])?;
            bind_guard(&mut stmt, job_id, worker_id, attempt)?;
            let mut rows = stmt.raw_query();
            match rows.next()? {
                Some(r) => Some(DeadRow::from_row(r)?),
                None => None,
            }
        };
        // Only a row this DELETE actually removed may become a dead row.
        let Some(row) = row else {
            return Ok(0);
        };
        row.insert_dead(conn, error)?;
        Ok(1)
    })
}

/// A row just removed from `_honker_live` by `DELETE ... RETURNING id,
/// queue, payload, priority, run_at, max_attempts, attempts,
/// created_at`, on its way to `_honker_dead`.
struct DeadRow {
    id: i64,
    queue: String,
    payload: String,
    priority: i64,
    run_at: i64,
    max_attempts: i64,
    attempts: i64,
    created_at: i64,
}

impl DeadRow {
    fn from_row(r: &rusqlite::Row<'_>) -> rusqlite::Result<Self> {
        Ok(Self {
            id: r.get(0)?,
            queue: r.get(1)?,
            payload: r.get(2)?,
            priority: r.get(3)?,
            run_at: r.get(4)?,
            max_attempts: r.get(5)?,
            attempts: r.get(6)?,
            created_at: r.get(7)?,
        })
    }

    fn insert_dead(&self, conn: &Connection, error: &str) -> rusqlite::Result<()> {
        conn.execute(
            "INSERT INTO _honker_dead
               (id, queue, payload, priority, run_at, max_attempts,
                attempts, last_error, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
            rusqlite::params![
                self.id,
                self.queue,
                self.payload,
                self.priority,
                self.run_at,
                self.max_attempts,
                self.attempts,
                error,
                self.created_at
            ],
        )?;
        Ok(())
    }
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
///
/// Unfenced: worker_id plus the lease. See [`fail_fenced`].
pub fn fail(conn: &Connection, job_id: i64, worker_id: &str, error: &str) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_fail", || {
        fail_inner(conn, job_id, worker_id, error, None)
    })
}

/// Fenced [`fail`]. The guard is `id`, `worker_id`, `attempts =
/// attempt` and `state = 'processing'`, with no lease check; see
/// [`ack_fenced`].
pub fn fail_fenced(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    error: &str,
    attempt: i64,
) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_fail", || {
        fail_inner(conn, job_id, worker_id, error, Some(attempt))
    })
}

const FAIL_SQL: [&str; 2] = [
    "DELETE FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2
       AND claim_expires_at >= unixepoch()
     RETURNING id, queue, payload, priority, run_at, max_attempts,
               attempts, created_at",
    "DELETE FROM _honker_live
     WHERE id = ?1 AND worker_id = ?2 AND attempts = ?3
       AND state = 'processing'
     RETURNING id, queue, payload, priority, run_at, max_attempts,
               attempts, created_at",
];

fn fail_inner(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    error: &str,
    attempt: Option<i64>,
) -> rusqlite::Result<i64> {
    let row = {
        let mut stmt = conn.prepare_cached(FAIL_SQL[usize::from(attempt.is_some())])?;
        bind_guard(&mut stmt, job_id, worker_id, attempt)?;
        let mut rows = stmt.raw_query();
        match rows.next()? {
            Some(r) => Some(DeadRow::from_row(r)?),
            None => None,
        }
    };
    let Some(row) = row else {
        return Ok(0);
    };
    row.insert_dead(conn, error)?;
    Ok(1)
}

/// Cancel a job by id. Removes scheduled, pending or processing rows from
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
        "DELETE FROM _honker_live
          WHERE id = ?1 AND state IN ('pending', 'scheduled', 'processing')",
        rusqlite::params![job_id],
    )?;
    Ok(n as i64)
}

/// Cancel a job by id, but only if it belongs to `queue`. Same
/// semantics as [`cancel`] otherwise: 1 if a scheduled, pending or processing row
/// was removed, 0 otherwise, idempotent.
///
/// A job in another queue is a miss, not an error — the caller gets 0,
/// the same answer it gets for an id that was already ack'd. That
/// matches how a queue handle treats a foreign id everywhere else.
///
/// The queue check is part of the DELETE, not a read before it. A
/// `SELECT queue` followed by a `DELETE` leaves a window where a
/// concurrent claim can change the row between the two statements; one
/// statement has no such window.
pub fn cancel_in_queue(conn: &Connection, queue: &str, job_id: i64) -> rusqlite::Result<i64> {
    let n = conn.execute(
        "DELETE FROM _honker_live
          WHERE queue = ?1 AND id = ?2 AND state IN ('pending', 'scheduled', 'processing')",
        rusqlite::params![queue, job_id],
    )?;
    Ok(n as i64)
}

/// SQL that answers "does the loaded honker extension have the
/// queue-scoped `honker_cancel(queue, job_id)`?" — returns 1 or 0.
///
/// Calling `honker_cancel` at an arity the loaded extension does not
/// have is a hard SQLite error ("wrong number of arguments"), not a
/// fallback, and there is no `honker_version()` to ask first. A binding
/// built for the 2-arg form and running against an older vendored
/// `libhonker_ext` would therefore fail at cancel time in production.
/// `pragma_function_list` reports each arity as its own row, so this
/// one cheap query at connect time turns that into a startup check.
///
/// Bindings that talk to the extension over SQL (Go, Ruby, .NET, C++,
/// Elixir, Bun) run this string verbatim; Rust-side bindings can call
/// [`has_queue_scoped_cancel`] instead.
///
/// If the query itself raises, that is "cannot tell", not "absent" —
/// a SQLite built with SQLITE_OMIT_INTROSPECTION_PRAGMAS has no
/// `pragma_function_list` to read. Surface the error. A binding that
/// rescues it into `false` reports an old extension while running a
/// new one, which is the exact failure this probe exists to prevent.
pub const CANCEL_QUEUE_SCOPED_PROBE_SQL: &str =
    "SELECT EXISTS(SELECT 1 FROM pragma_function_list WHERE name = 'honker_cancel' AND narg = 2)";

/// Run [`CANCEL_QUEUE_SCOPED_PROBE_SQL`] on `conn`.
///
/// Errors are propagated, not flattened into `false`: a SQLite build
/// compiled without the introspection pragmas cannot answer this
/// question, and reporting "no queue-scoped cancel" for "cannot tell"
/// would send a binding down the wrong migration path silently.
pub fn has_queue_scoped_cancel(conn: &Connection) -> rusqlite::Result<bool> {
    conn.query_row(CANCEL_QUEUE_SCOPED_PROBE_SQL, [], |r| r.get(0))
}

/// Read a single job row by id. Returns a JSON object on success or
/// the empty string on miss (job ack'd, dead'd, or never existed).
/// Pure read — does not change state.
///
/// `claimed_at` is null for a job that has never been claimed, and for
/// a job that was already in flight when an existing database migrated
/// (the migration adds the column without backfilling). Otherwise it is
/// the start of the current claim, and it is only meaningful while
/// `claim_expires_at >= unixepoch()` — see `claim_batch`.
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
        Option<i64>,
    )> = conn
        .query_row(
            "SELECT id, queue, payload, state, priority, run_at, worker_id,
                    claim_expires_at, attempts, max_attempts, created_at, expires_at,
                    claimed_at
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
                    r.get(12)?,
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
        claimed_at,
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
        "claimed_at": claimed_at,
    })
    .to_string())
}

/// Extend the current claim by `extend_s` seconds. Returns 1 if the
/// heartbeat landed, 0 if we're not the holder (either the row is
/// in a different state or worker_id doesn't match).
///
/// Moves `claim_expires_at` only. `claimed_at` must NOT be touched
/// here: it marks when the current attempt started, and a heartbeat
/// does not start a new attempt. Refreshing it would make a
/// long-running job look like it just began, which is exactly the
/// blind spot `claimed_at` exists to fix.
///
/// Unfenced: worker_id plus the lease. See [`heartbeat_fenced`].
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

/// Fenced [`heartbeat`]: sets `claim_expires_at = now + extend_s` if
/// the row is still this claim (`id`, `worker_id`, `attempts =
/// attempt`, `state = 'processing'`). Returns 1 if extended, else 0.
///
/// No lease check, on purpose. The unfenced form needs one because
/// worker_id alone cannot tell a reclaimer from the stale holder. The
/// token can: a reclaim bumps `attempts`, so a stale heartbeat matches
/// nothing. A heartbeat after the lease lapsed but before anyone
/// reclaimed the job revives the lease, which is correct: the holder
/// is still working and nobody else has started. `claimed_at` is left
/// alone, as in [`heartbeat`].
pub fn heartbeat_fenced(
    conn: &Connection,
    job_id: i64,
    worker_id: &str,
    extend_s: i64,
    attempt: i64,
) -> rusqlite::Result<i64> {
    let updated = conn.execute(
        "UPDATE _honker_live
         SET claim_expires_at = unixepoch() + ?3
         WHERE id = ?1 AND worker_id = ?2 AND attempts = ?4
           AND state = 'processing'",
        rusqlite::params![job_id, worker_id, extend_s, attempt],
    )?;
    Ok(updated as i64)
}

// ---------------------------------------------------------------------
// Task expiration
// ---------------------------------------------------------------------

/// Move expired rows from `_honker_live` to `_honker_dead` with
/// `last_error='expired'`. Returns count moved.
///
/// Same rows as the expiry step of [`claim_batch`], with no row limit:
/// `scheduled` and `pending` rows, and `processing` rows whose lease
/// lapsed. A `processing` row with a valid lease is left for its owner.
/// Every claim already does this for its queue, so calling it is
/// optional; it stays for compatibility and for queues nobody claims.
///
/// Runs in a SAVEPOINT for the same reason as the claim's moves: DELETE
/// ... RETURNING, then decode, then INSERT. Measured before the
/// savepoint: a decode failure and a failing dead-table INSERT both
/// gave live=0, dead=0.
pub fn sweep_expired(conn: &Connection, queue: &str) -> rusqlite::Result<i64> {
    in_savepoint(conn, "honker_sweep_expired", || {
        let now = now_unix(conn)?;
        move_to_dead(conn, EXPIRE_IDS, queue, now, -1, "expired")
    })
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
/// row and applied to every job `scheduler_tick` enqueues for it. It
/// must be at least 1 (an error otherwise; it used to be clamped to 1).
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
    check_max_attempts("honker_scheduler_register", max_attempts)?;
    super::validate_json_payload(payload)?;
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

/// Record one boundary of a schedule whose stored payload is not a
/// valid JSON payload, instead of enqueueing it: a `_honker_dead` row
/// with the schedule's queue, payload, priority and max_attempts,
/// `run_at` = the boundary, `attempts` = 0 and `last_error` = `error`.
/// Returns the row's id.
///
/// The id comes from `_honker_live`'s AUTOINCREMENT, like every other
/// dead row's, so it cannot collide with a job that dies later. The
/// placeholder live row is deleted again inside the tick's savepoint;
/// no other connection sees it.
fn dead_letter_unfireable(
    conn: &Connection,
    now: i64,
    task: &DueTask,
    fire_at: i64,
    error: &str,
) -> rusqlite::Result<i64> {
    let id: i64 = conn
        .prepare_cached(
            "INSERT INTO _honker_live
               (queue, payload, priority, run_at, max_attempts, created_at)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6)
             RETURNING id",
        )?
        .query_row(
            rusqlite::params![
                task.queue,
                task.payload,
                task.priority,
                fire_at,
                task.max_attempts,
                now
            ],
            |r| r.get(0),
        )?;
    conn.prepare_cached("DELETE FROM _honker_live WHERE id = ?1")?
        .execute(rusqlite::params![id])?;
    conn.prepare_cached(
        "INSERT INTO _honker_dead
           (id, queue, payload, priority, run_at, max_attempts,
            attempts, last_error, created_at, died_at)
         VALUES (?1, ?2, ?3, ?4, ?5, ?6, 0, ?7, ?8, ?8)",
    )?
    .execute(rusqlite::params![
        id,
        task.queue,
        task.payload,
        task.priority,
        fire_at,
        task.max_attempts,
        error,
        now
    ])?;
    Ok(id)
}

/// For each registered task whose `next_fire_at <= now_unix`,
/// enqueue the payload into its queue and advance `next_fire_at`
/// to the next boundary. Keeps advancing within one tick while
/// boundaries remain in the past (catches up after a scheduler
/// outage), up to [`SCHEDULER_MAX_CATCHUP_FIRES`] per task.
/// Returns a JSON array of `{name, queue, fire_at, job_id}` fires,
/// ordered by task name and then by `fire_at`.
///
/// A task whose stored payload fails [`super::validate_json_payload`]
/// (written by raw SQL or before the JSON payload contract) is not
/// enqueued and does not fail the tick. Each of its due boundaries
/// becomes one `_honker_dead` row whose `last_error` names the schedule
/// ([`dead_letter_unfireable`]), and its `next_fire_at` advances like a
/// fired task's. Those boundaries are not in the returned array. A bad
/// row cannot stop the other schedules, and is reported once per
/// boundary instead of on every tick.
///
/// Atomic on its own, in autocommit or inside a caller's transaction
/// (#173). Everything runs in one savepoint, and its first statement is
/// [`TICK_DUE_SQL`]: a no-op UPDATE that takes the write lock and
/// returns the due tasks from the snapshot it locked. Reading the due
/// tasks before taking the lock was the bug:
///
///   * Two ticks could both read the same due boundary, and both
///     enqueued it. Now the second tick waits on the lock
///     (`busy_timeout`) and then sees the advanced `next_fire_at`.
///   * Under WAL, a commit from another connection between that read
///     and the first enqueue failed the tick with "database is locked"
///     (SQLITE_BUSY_SNAPSHOT), which `busy_timeout` does not retry.
///     With no read before the write, there is no stale snapshot.
///   * In autocommit every enqueue committed on its own, so a failure
///     part-way left jobs whose boundary was not advanced, and the next
///     tick enqueued them again. Now a failure rolls back the whole
///     tick: no job is enqueued and no `next_fire_at` moves, and the
///     error goes to the caller. The next tick fires those boundaries.
///
/// The clock is read once: every job this tick enqueues gets the same
/// `unixepoch()` for its `run_at` and `expires_at`.
///
/// A caller no longer needs its own transaction around the tick. One
/// that has one keeps it: a failure rolls back to the savepoint only.
/// Inside a transaction that has already read, SQLite can still refuse
/// the tick's first write with SQLITE_BUSY_SNAPSHOT; that snapshot
/// belongs to the caller.
pub fn scheduler_tick(conn: &Connection, now_unix: i64) -> rusqlite::Result<String> {
    in_savepoint(conn, "honker_scheduler_tick", || {
        scheduler_tick_inner(conn, now_unix)
    })
}

/// Takes the write lock and returns the due tasks. The `SET` changes
/// nothing; `scheduler_tick_inner` writes the real `next_fire_at`.
/// An UPDATE takes the write lock even when no row matches, so an idle
/// tick serializes with other writers too. It writes no page then, so
/// its commit does not wake `data_version` watchers.
const TICK_DUE_SQL: &str = "UPDATE _honker_scheduler_tasks
       SET next_fire_at = next_fire_at
     WHERE enabled = 1 AND next_fire_at <= ?1
 RETURNING name, queue, cron_expr, payload, priority, expires_s,
           next_fire_at, COALESCE(max_attempts, 3)";

struct DueTask {
    name: String,
    queue: String,
    cron_expr: String,
    payload: String,
    priority: i64,
    expires_s: Option<i64>,
    next_fire_at: i64,
    max_attempts: i64,
}

fn scheduler_tick_inner(conn: &Connection, tick_at: i64) -> rusqlite::Result<String> {
    let mut tasks = conn
        .prepare_cached(TICK_DUE_SQL)?
        .query_map(rusqlite::params![tick_at], |r| {
            Ok(DueTask {
                name: r.get(0)?,
                queue: r.get(1)?,
                cron_expr: r.get(2)?,
                payload: r.get(3)?,
                priority: r.get(4)?,
                expires_s: r.get(5)?,
                next_fire_at: r.get(6)?,
                max_attempts: r.get(7)?,
            })
        })?
        .collect::<Result<Vec<_>, _>>()?;
    if tasks.is_empty() {
        return Ok("[]".to_string());
    }
    // RETURNING order is unspecified.
    tasks.sort_by(|a, b| a.name.cmp(&b.name));
    // After the UPDATE, so this reads no table before the write lock.
    let now = now_unix(conn)?;
    let mut out = Vec::new();
    for task in tasks {
        // A schedule row written before the JSON payload contract (raw
        // SQL, or an older build) can hold text `enqueue` now rejects.
        // Checked once per task: the payload is the same at every boundary.
        let bad_payload = match super::validate_json_payload(&task.payload) {
            Ok(()) => None,
            Err(rusqlite::Error::UserFunctionError(e)) => Some(format!(
                "{e} (schedule {:?}); fire not enqueued, fix the payload \
                 with honker_scheduler_update",
                task.name
            )),
            Err(e) => return Err(e),
        };
        let mut next_fire_at = task.next_fire_at;
        let mut fires_this_task: i64 = 0;
        while next_fire_at <= tick_at {
            if fires_this_task >= SCHEDULER_MAX_CATCHUP_FIRES {
                // Skip the remaining backlog. Resume from the next
                // boundary strictly after now so we don't immediately
                // re-enter the catch-up loop on the next tick.
                // Intermediate boundaries are intentionally never
                // enqueued (see SCHEDULER_MAX_CATCHUP_FIRES docs).
                next_fire_at =
                    super::cron::next_after_unix(&task.cron_expr, tick_at).map_err(to_sql_err)?;
                break;
            }
            check_max_attempts("honker_scheduler_tick", task.max_attempts)?;
            match &bad_payload {
                // Enqueue at this boundary. `run_at` is NULL (claimable
                // immediately); `expires` is the task's expires_s if set.
                // max_attempts comes from the schedule row, not a constant.
                None => {
                    let job_id = enqueue_at(
                        conn,
                        now,
                        &task.queue,
                        &task.payload,
                        None,
                        None,
                        task.priority,
                        task.max_attempts,
                        task.expires_s,
                    )?;
                    out.push(json!({
                        "name": task.name,
                        "queue": task.queue,
                        "fire_at": next_fire_at,
                        "job_id": job_id,
                    }));
                }
                // This boundary can never become a job a worker decodes.
                // Record it in `_honker_dead` and advance as if it fired:
                // the other tasks keep firing, and each skipped boundary
                // is reported exactly once.
                Some(error) => {
                    dead_letter_unfireable(conn, now, &task, next_fire_at, error)?;
                }
            }
            fires_this_task += 1;
            // Advance to the next boundary strictly after this one.
            next_fire_at =
                super::cron::next_after_unix(&task.cron_expr, next_fire_at).map_err(to_sql_err)?;
        }
        conn.prepare_cached(
            "UPDATE _honker_scheduler_tasks
             SET next_fire_at = ?2 WHERE name = ?1",
        )?
        .execute(rusqlite::params![task.name, next_fire_at])?;
    }
    Ok(Value::Array(out).to_string())
}

pub fn scheduler_soonest(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row(
        "SELECT COALESCE(MIN(next_fire_at), 0) FROM _honker_scheduler_tasks WHERE enabled = 1",
        [],
        |r| r.get(0),
    )
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
/// row was updated, 0 if it doesn't exist. A new `max_attempts` below
/// 1 is an error (it used to be clamped to 1); `Some(None)` resets it
/// to 3.
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
    if let Some(Some(m)) = max_attempts {
        check_max_attempts("honker_scheduler_update", m)?;
    }
    if let Some(payload) = payload {
        super::validate_json_payload(payload)?;
    }
    // Verify exists first so we can return 0 cleanly without dynamic SQL gymnastics.
    let exists: bool = conn
        .query_row(
            "SELECT 1 FROM _honker_scheduler_tasks WHERE name = ?1",
            rusqlite::params![name],
            |_| Ok(true),
        )
        .optional()?
        .is_some();
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
    super::validate_json_payload(value)?;
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
    super::validate_json_payload(payload)?;
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
        .optional()?
        .unwrap_or(0))
}

// ---------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------

fn now_unix(conn: &Connection) -> rusqlite::Result<i64> {
    conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
}

/// Reject an attempt budget below 1. Such a job could never be
/// claimed: claim needs `attempts < max_attempts`, and `attempts` starts
/// at 0. `func` is the public SQL function, named in the error.
fn check_max_attempts(func: &str, max_attempts: i64) -> rusqlite::Result<()> {
    if max_attempts < 1 {
        return Err(to_sql_err(format!(
            "{func}: max_attempts must be at least 1, got {max_attempts}"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod payload_tests {
    use super::*;
    use crate::{attach_honker_functions, bootstrap_honker_schema};
    use rusqlite::params;

    // Bare scalars are JSON. A stricter validator is most likely to break
    // these by accident, and #152 settled that they must keep working.
    const VALID_JSON_PAYLOADS: [&str; 6] = ["{}", "[]", "42", "\"str\"", "true", "null"];

    // Valid by the JSON grammar, but no decoder can represent them: a lone
    // surrogate is not valid UTF-8, and 1e999 overflows f64. A structural
    // scan accepts all of these; `serde_json::Value` -- the read side --
    // rejects them. Accepting them at enqueue is how a Python producer
    // creates a job a Rust consumer cannot decode.
    const UNDECODABLE_JSON_PAYLOADS: [&str; 5] = [
        r#""\ud800""#,
        r#"{"a":"\ud800"}"#,
        "1e999",
        r#"{"a":1e999}"#,
        "-1e999",
    ];

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        attach_honker_functions(&conn).unwrap();
        conn
    }

    fn assert_payload_error<T>(result: rusqlite::Result<T>) {
        let err = result.err().expect("non-JSON payload must be rejected");
        let text = err.to_string();
        assert!(
            text.contains("honker: payload must be valid JSON"),
            "expected the JSON payload contract error, got: {err}"
        );
        assert!(
            text.contains("line 1 column"),
            "the error must pass serde's own message through so it names \
             the real problem, got: {err}"
        );
    }

    /// Text that parses as JSON but cannot be decoded gets its own message.
    /// Telling this caller their payload "must be valid JSON" would send
    /// them hunting for a syntax error they do not have.
    fn assert_undecodable_payload_error<T>(result: rusqlite::Result<T>, payload: &str) {
        let err = result
            .err()
            .unwrap_or_else(|| panic!("undecodable JSON payload must be rejected: {payload}"));
        let text = err.to_string();
        assert!(
            text.contains("honker: payload is valid JSON but cannot be decoded"),
            "expected the undecodable-JSON error for {payload}, got: {err}"
        );
        assert!(
            text.contains("line 1 column"),
            "the error must pass serde's own message through for {payload}, got: {err}"
        );
    }

    #[test]
    fn orm_select_enqueue_rejects_non_json_payload() {
        let conn = db();
        let result = conn.query_row(
            "SELECT honker_enqueue('emails', ?1, NULL, NULL, 0, 3, NULL)",
            ["not json"],
            |r| r.get::<_, i64>(0),
        );
        assert_payload_error(result);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn enqueue_accepts_every_json_payload_shape() {
        let conn = db();
        for payload in VALID_JSON_PAYLOADS {
            let id: i64 = conn
                .query_row(
                    "SELECT honker_enqueue('emails', ?1, NULL, NULL, 0, 3, NULL)",
                    [payload],
                    |r| r.get(0),
                )
                .unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT payload FROM _honker_live WHERE id = ?1",
                    [id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stored, payload);
        }
    }

    #[test]
    fn stream_publish_rejects_non_json_payload() {
        let conn = db();
        let result = conn.query_row(
            "SELECT honker_stream_publish('orders', NULL, ?1)",
            ["not json"],
            |r| r.get::<_, i64>(0),
        );
        assert_payload_error(result);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_stream", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn stream_publish_accepts_every_json_payload_shape() {
        let conn = db();
        for payload in VALID_JSON_PAYLOADS {
            let offset: i64 = conn
                .query_row(
                    "SELECT honker_stream_publish('orders', NULL, ?1)",
                    [payload],
                    |r| r.get(0),
                )
                .unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT payload FROM _honker_stream WHERE offset = ?1",
                    [offset],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stored, payload);
        }
    }

    #[test]
    fn result_save_rejects_non_json_payload() {
        let conn = db();
        let result = conn.query_row("SELECT honker_result_save(1, ?1, 0)", ["not json"], |r| {
            r.get::<_, i64>(0)
        });
        assert_payload_error(result);
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_results", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn result_save_accepts_every_json_payload_shape() {
        let conn = db();
        for (job_id, payload) in VALID_JSON_PAYLOADS.into_iter().enumerate() {
            let job_id = job_id as i64;
            conn.query_row(
                "SELECT honker_result_save(?1, ?2, 0)",
                params![job_id, payload],
                |r| r.get::<_, i64>(0),
            )
            .unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT value FROM _honker_results WHERE job_id = ?1",
                    [job_id],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stored, payload);
        }
    }

    #[test]
    fn scheduler_register_rejects_non_json_payload() {
        let conn = db();
        for sql in [
            "SELECT honker_scheduler_register('nightly-6', 'backups', '@every 1m', ?1, 0, NULL)",
            "SELECT honker_scheduler_register('nightly-7', 'backups', '@every 1m', ?1, 0, NULL, 3)",
        ] {
            let result = conn.query_row(sql, ["not json"], |r| r.get::<_, i64>(0));
            assert_payload_error(result);
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_scheduler_tasks", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn scheduler_register_accepts_every_json_payload_shape() {
        let conn = db();
        for (index, payload) in VALID_JSON_PAYLOADS.into_iter().enumerate() {
            let sql = if index % 2 == 0 {
                "SELECT honker_scheduler_register('nightly', 'backups', '@every 1m', ?1, 0, NULL)"
            } else {
                "SELECT honker_scheduler_register('nightly', 'backups', '@every 1m', ?1, 0, NULL, 3)"
            };
            conn.query_row(sql, [payload], |r| r.get::<_, i64>(0))
                .unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT payload FROM _honker_scheduler_tasks WHERE name = 'nightly'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stored, payload);
        }
    }

    #[test]
    fn scheduler_update_rejects_non_json_payload() {
        let conn = db();
        conn.query_row(
            "SELECT honker_scheduler_register('nightly', 'backups', '@every 1m', '{}', 0, NULL, 3)",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
        for sql in [
            "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0)",
            "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0, NULL, 0)",
        ] {
            let result = conn.query_row(sql, ["not json"], |r| r.get::<_, i64>(0));
            assert_payload_error(result);
        }
        let stored: String = conn
            .query_row(
                "SELECT payload FROM _honker_scheduler_tasks WHERE name = 'nightly'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stored, "{}", "rejected payload must not be written");
    }

    #[test]
    fn orm_select_enqueue_rejects_undecodable_json_payload() {
        let conn = db();
        for payload in UNDECODABLE_JSON_PAYLOADS {
            let result = conn.query_row(
                "SELECT honker_enqueue('emails', ?1, NULL, NULL, 0, 3, NULL)",
                [payload],
                |r| r.get::<_, i64>(0),
            );
            assert_undecodable_payload_error(result, payload);
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn stream_publish_rejects_undecodable_json_payload() {
        let conn = db();
        for payload in UNDECODABLE_JSON_PAYLOADS {
            let result = conn.query_row(
                "SELECT honker_stream_publish('orders', NULL, ?1)",
                [payload],
                |r| r.get::<_, i64>(0),
            );
            assert_undecodable_payload_error(result, payload);
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_stream", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn result_save_rejects_undecodable_json_payload() {
        let conn = db();
        for payload in UNDECODABLE_JSON_PAYLOADS {
            let result = conn.query_row("SELECT honker_result_save(1, ?1, 0)", [payload], |r| {
                r.get::<_, i64>(0)
            });
            assert_undecodable_payload_error(result, payload);
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_results", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn scheduler_register_rejects_undecodable_json_payload() {
        let conn = db();
        for payload in UNDECODABLE_JSON_PAYLOADS {
            for sql in [
                "SELECT honker_scheduler_register('nightly-6', 'backups', '@every 1m', ?1, 0, NULL)",
                "SELECT honker_scheduler_register('nightly-7', 'backups', '@every 1m', ?1, 0, NULL, 3)",
            ] {
                let result = conn.query_row(sql, [payload], |r| r.get::<_, i64>(0));
                assert_undecodable_payload_error(result, payload);
            }
        }
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_scheduler_tasks", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    #[test]
    fn scheduler_update_rejects_undecodable_json_payload() {
        let conn = db();
        conn.query_row(
            "SELECT honker_scheduler_register('nightly', 'backups', '@every 1m', '{}', 0, NULL, 3)",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
        for payload in UNDECODABLE_JSON_PAYLOADS {
            for sql in [
                "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0)",
                "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0, NULL, 0)",
            ] {
                let result = conn.query_row(sql, [payload], |r| r.get::<_, i64>(0));
                assert_undecodable_payload_error(result, payload);
            }
        }
        let stored: String = conn
            .query_row(
                "SELECT payload FROM _honker_scheduler_tasks WHERE name = 'nightly'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stored, "{}", "rejected payload must not be written");
    }

    /// The stricter parse also bounds nesting, which the structural scan did
    /// not -- 50,000 nested arrays used to enqueue. A consumer decoding with
    /// `serde_json` hits the same bound, so accepting these was the same
    /// undecodable-job bug in a third shape.
    ///
    /// Deliberately does not pin the exact limit: `serde_json` owns that
    /// number and may move it. 4096 is far enough past any plausible limit to
    /// stay a rejection, and 32 is shallow enough to stay accepted.
    #[test]
    fn enqueue_rejects_nesting_deeper_than_the_decoder_accepts() {
        let conn = db();
        let payload = format!("{}{}", "[".repeat(4096), "]".repeat(4096));
        let result = conn.query_row(
            "SELECT honker_enqueue('emails', ?1, NULL, NULL, 0, 3, NULL)",
            [payload.as_str()],
            |r| r.get::<_, i64>(0),
        );
        assert_undecodable_payload_error(result, "4096 nested arrays");
        let count: i64 = conn
            .query_row("SELECT COUNT(*) FROM _honker_live", [], |r| r.get(0))
            .unwrap();
        assert_eq!(count, 0, "rejected payload must not be written");
    }

    /// The guard against over-tightening: ordinary nesting must still enqueue.
    #[test]
    fn enqueue_accepts_ordinary_nesting() {
        let conn = db();
        let payload = format!("{}{}", "[".repeat(32), "]".repeat(32));
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', ?1, NULL, NULL, 0, 3, NULL)",
                [payload.as_str()],
                |r| r.get(0),
            )
            .unwrap();
        let stored: String = conn
            .query_row(
                "SELECT payload FROM _honker_live WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(stored, payload);
    }

    /// Plant schedule rows the way a pre-contract build or raw SQL could
    /// have written them: no validation, all due at `next_fire_at = 1`.
    fn plant_legacy_schedules(conn: &Connection, rows: &[(&str, &str)]) {
        for (name, payload) in rows {
            conn.execute(
                "INSERT INTO _honker_scheduler_tasks
                   (name, queue, cron_expr, payload, priority, next_fire_at,
                    enabled, max_attempts)
                 VALUES (?1, 'backups', '@every 1m', ?2, 0, 1, 1, 3)",
                params![name, payload],
            )
            .unwrap();
        }
    }

    fn tick(conn: &Connection, at: i64) -> rusqlite::Result<Vec<serde_json::Value>> {
        let text: String =
            conn.query_row("SELECT honker_scheduler_tick(?1)", [at], |r| r.get(0))?;
        Ok(serde_json::from_str(&text).unwrap())
    }

    fn count(conn: &Connection, sql: &str) -> i64 {
        conn.query_row(sql, [], |r| r.get(0)).unwrap()
    }

    /// Existing rows are not validated retroactively, so a schedule
    /// registered before this contract can still hold text `enqueue` now
    /// rejects. Such a schedule must not stop the others, and must not
    /// vanish: each of its due boundaries becomes one `_honker_dead` row
    /// whose `last_error` names the schedule, and its `next_fire_at`
    /// advances exactly like a healthy schedule's. The healthy schedule
    /// fires every boundary once, the bad one is reported once per
    /// boundary, and nothing repeats on the next tick.
    #[test]
    fn a_legacy_schedule_payload_is_dead_lettered_without_blocking_the_tick() {
        let conn = db();
        // 'aaa'/'bbb' sort first, so the bad rows are reached before the
        // healthy one.
        plant_legacy_schedules(
            &conn,
            &[
                ("aaa-legacy", "not json"),
                ("bbb-legacy-undecodable", r#""\ud800""#),
                ("zzz-healthy", "{}"),
            ],
        );

        let fires = tick(&conn, 120).expect("a bad schedule row must not fail the tick");
        assert!(!fires.is_empty());
        assert!(
            fires.iter().all(|f| f["name"] == "zzz-healthy"),
            "only the healthy schedule enqueues: {fires:?}"
        );
        let healthy = fires.len() as i64;
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM _honker_live"), healthy);
        for name in ["aaa-legacy", "bbb-legacy-undecodable"] {
            let dead = count(
                &conn,
                &format!(
                    "SELECT COUNT(*) FROM _honker_dead WHERE queue = 'backups' \
                     AND last_error LIKE '%\"{name}\"%' AND attempts = 0"
                ),
            );
            assert_eq!(
                dead, healthy,
                "{name}: one dead row per skipped boundary, like the healthy fires"
            );
        }
        let not_json: String = conn
            .query_row(
                "SELECT last_error FROM _honker_dead WHERE payload = 'not json' LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            not_json.contains("honker: payload must be valid JSON")
                && not_json.contains("honker_scheduler_update"),
            "{not_json}"
        );
        let undecodable: String = conn
            .query_row(
                "SELECT last_error FROM _honker_dead WHERE payload LIKE '%ud800%' LIMIT 1",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(undecodable.contains("cannot be decoded"), "{undecodable}");
        // The dead rows' run_at values are exactly the healthy fire_at values.
        let fire_ats: Vec<i64> = fires
            .iter()
            .map(|f| f["fire_at"].as_i64().unwrap())
            .collect();
        let dead_run_ats: Vec<i64> = conn
            .prepare("SELECT run_at FROM _honker_dead WHERE payload = 'not json' ORDER BY run_at")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert_eq!(dead_run_ats, fire_ats);
        // Every task advanced to the same next boundary.
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(DISTINCT next_fire_at) FROM _honker_scheduler_tasks \
                 WHERE next_fire_at > 120"
            ),
            1
        );
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM _honker_scheduler_tasks WHERE next_fire_at > 120"
            ),
            3
        );

        // Same instant again: nothing is due, nothing repeats.
        assert!(tick(&conn, 120).unwrap().is_empty());
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM _honker_live"), healthy);
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM _honker_dead"),
            2 * healthy
        );

        // Next boundary: the healthy one fires again, the bad ones are
        // reported again, once each.
        let soonest = count(&conn, "SELECT honker_scheduler_soonest()");
        let fires = tick(&conn, soonest).unwrap();
        assert_eq!(fires.len(), 1);
        assert_eq!(fires[0]["name"], "zzz-healthy");
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM _honker_dead"),
            2 * healthy + 2
        );

        // Fixing the payload through the public API makes it fire.
        conn.query_row(
            "SELECT honker_scheduler_update('aaa-legacy', NULL, '{\"fixed\":true}', NULL, NULL, 0)",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
        let soonest = count(&conn, "SELECT honker_scheduler_soonest()");
        let fires = tick(&conn, soonest).unwrap();
        let names: Vec<&str> = fires.iter().map(|f| f["name"].as_str().unwrap()).collect();
        assert_eq!(names, ["aaa-legacy", "zzz-healthy"]);
    }

    /// A dead-lettered fire takes its id from `_honker_live`'s sequence, so
    /// a job that dies later cannot collide with it in `_honker_dead`.
    #[test]
    fn a_dead_lettered_fire_does_not_collide_with_later_dead_jobs() {
        let conn = db();
        plant_legacy_schedules(&conn, &[("aaa-legacy", "not json")]);
        tick(&conn, 120).unwrap();
        let dead_ids: Vec<i64> = conn
            .prepare("SELECT id FROM _honker_dead ORDER BY id")
            .unwrap()
            .query_map([], |r| r.get(0))
            .unwrap()
            .collect::<Result<_, _>>()
            .unwrap();
        assert!(!dead_ids.is_empty());
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM _honker_live"), 0);
        let job: i64 = conn
            .query_row(
                "SELECT honker_enqueue('backups', '{}', NULL, NULL, 0, 1, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(job > *dead_ids.last().unwrap());
        let claimed: String = conn
            .query_row(
                "SELECT honker_claim_batch('backups', 'w', 1, 60)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert!(claimed.contains(&format!("\"id\":{job}")), "{claimed}");
        let moved: i64 = conn
            .query_row("SELECT honker_fail(?1, 'w', 'boom')", [job], |r| r.get(0))
            .unwrap();
        assert_eq!(moved, 1);
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM _honker_dead"),
            dead_ids.len() as i64 + 1
        );
    }

    /// The tick is atomic (#173). If recording a skipped fire fails, the
    /// healthy fires of the same tick roll back with it and no
    /// `next_fire_at` moves, so the next tick fires them, once.
    #[test]
    fn a_failed_dead_letter_rolls_back_the_whole_tick() {
        let conn = db();
        plant_legacy_schedules(&conn, &[("aaa-legacy", "not json"), ("zzz-healthy", "{}")]);
        conn.execute_batch(
            "CREATE TRIGGER test_block_dead BEFORE INSERT ON _honker_dead
             BEGIN SELECT RAISE(ABORT, 'dead insert blocked'); END;",
        )
        .unwrap();
        let err = tick(&conn, 120).expect_err("the dead-letter failure reaches the caller");
        assert!(err.to_string().contains("dead insert blocked"), "{err}");
        assert_eq!(count(&conn, "SELECT COUNT(*) FROM _honker_live"), 0);
        assert_eq!(
            count(
                &conn,
                "SELECT COUNT(*) FROM _honker_scheduler_tasks WHERE next_fire_at = 1"
            ),
            2
        );
        conn.execute_batch("DROP TRIGGER test_block_dead").unwrap();
        let fires = tick(&conn, 120).unwrap();
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM _honker_live"),
            fires.len() as i64
        );
        assert_eq!(
            count(&conn, "SELECT COUNT(*) FROM _honker_dead"),
            fires.len() as i64
        );
    }

    #[test]
    fn scheduler_update_accepts_every_json_payload_shape() {
        let conn = db();
        conn.query_row(
            "SELECT honker_scheduler_register('nightly', 'backups', '@every 1m', '{}', 0, NULL, 3)",
            [],
            |r| r.get::<_, i64>(0),
        )
        .unwrap();
        for (index, payload) in VALID_JSON_PAYLOADS.into_iter().enumerate() {
            let sql = if index % 2 == 0 {
                "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0)"
            } else {
                "SELECT honker_scheduler_update('nightly', NULL, ?1, NULL, NULL, 0, NULL, 0)"
            };
            conn.query_row(sql, [payload], |r| r.get::<_, i64>(0))
                .unwrap();
            let stored: String = conn
                .query_row(
                    "SELECT payload FROM _honker_scheduler_tasks WHERE name = 'nightly'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(stored, payload);
        }
    }
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
    //
    // claim_batch and sweep_expired now move rows with a set INSERT ...
    // SELECT and a DELETE (`move_to_dead`): nothing is decoded in Rust,
    // so a corrupt column moves with the row. What can still go wrong
    // is the dead INSERT failing, or the two statements disagreeing on
    // the rows; both must roll back.

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

    /// Turn a job into a claim whose holder vanished: `processing`, with
    /// a lease that lapsed 10 s ago.
    fn lapse(conn: &Connection, id: i64) {
        conn.execute(
            "UPDATE _honker_live SET state = 'processing', worker_id = 'gone',
                    claim_expires_at = unixepoch() - 10
              WHERE id = ?1",
            [id],
        )
        .unwrap();
    }

    /// Make the INSERT and the DELETE of a dead-letter move disagree:
    /// once a row is copied to `_honker_dead`, the live row stops
    /// matching (its lease looks valid and it no longer expires), so
    /// the DELETE misses it.
    fn make_copy_and_delete_disagree(conn: &Connection) {
        conn.execute_batch(
            "CREATE TRIGGER test_disagree AFTER INSERT ON _honker_dead
             BEGIN
               UPDATE _honker_live
                  SET claim_expires_at = unixepoch() + 100, expires_at = NULL
                WHERE id = NEW.id;
             END",
        )
        .unwrap();
    }

    /// A lapsed claim that has already used its whole attempt budget,
    /// so an ordinary claim dead-letters it.
    fn exhausted_lapsed_job(conn: &Connection) -> i64 {
        let id: i64 = conn
            .query_row(
                "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 1, NULL)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        conn.execute("UPDATE _honker_live SET attempts = 1 WHERE id = ?1", [id])
            .unwrap();
        lapse(conn, id);
        id
    }

    // The hot path: no fail() call involved, an ordinary claim triggers
    // it. A corrupt column no longer stops the move: the row lands in
    // _honker_dead whole and leaves _honker_live, nothing in between.
    #[test]
    fn claim_batch_moves_a_corrupt_row_whole() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        lapse(&conn, id);
        assert_eq!(claim_batch(&conn, "emails", "w1", 8, 300).unwrap(), "[]");
        assert_eq!(live_count(&conn, id), 0);
        let attempts: String = conn
            .query_row(
                "SELECT attempts FROM _honker_dead WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(attempts, "not-a-number", "copied as is");
    }

    // If the copy and the delete ever select different rows, the move
    // must roll back instead of losing or duplicating a job.
    #[test]
    fn claim_batch_rolls_back_when_copy_and_delete_disagree() {
        let conn = db();
        let a = exhausted_lapsed_job(&conn);
        let b = exhausted_lapsed_job(&conn);
        make_copy_and_delete_disagree(&conn);
        let err = claim_batch(&conn, "emails", "w1", 8, 300)
            .expect_err("a copy/delete mismatch must reach the caller");
        assert!(
            err.to_string().contains("copied 2 but deleted 0"),
            "got: {err}"
        );
        assert_eq!((live_count(&conn, a), live_count(&conn, b)), (1, 1));
        assert_eq!((dead_count(&conn, a), dead_count(&conn, b)), (0, 0));
        assert!(conn.is_autocommit());
    }

    #[test]
    fn claim_batch_rolls_back_when_the_dead_insert_fails() {
        let conn = db();
        let id = exhausted_lapsed_job(&conn);
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
        let exhausted = exhausted_lapsed_job(&conn);
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

    // sweep_expired: the same set move as the claim's expiry step.
    #[test]
    fn sweep_expired_rolls_back_when_copy_and_delete_disagree() {
        let conn = db();
        let id = undecodable_pending_job(&conn);
        conn.execute(
            "UPDATE _honker_live SET expires_at = unixepoch() - 10 WHERE id = ?1",
            [id],
        )
        .unwrap();
        make_copy_and_delete_disagree(&conn);
        let err = sweep_expired(&conn, "emails").expect_err("a mismatch must reach the caller");
        assert!(
            err.to_string().contains("copied 1 but deleted 0"),
            "got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the move must roll back, not lose the job"
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
    fn sql_honker_claim_batch_rolls_back_the_dead_letter_move() {
        let conn = db();
        let id = exhausted_lapsed_job(&conn);
        make_copy_and_delete_disagree(&conn);
        let err = conn
            .query_row(
                "SELECT honker_claim_batch('emails', 'w1', 8, 300)",
                [],
                |r| r.get::<_, String>(0),
            )
            .expect_err("the mismatch must surface through the SQL function too");
        assert!(
            err.to_string().contains("copied 1 but deleted 0"),
            "got: {err}"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "the dead-letter move must roll back on the binding path too"
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
        block_dead_inserts(&conn);
        let err = conn
            .query_row("SELECT honker_sweep_expired('emails')", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect_err("the insert failure must surface through the SQL function too");
        assert!(
            err.to_string().contains("dead insert blocked"),
            "got: {err}"
        );
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

    // A panic unwinds past every `match` arm in in_savepoint, so the
    // undo cannot live only on the error path. Measured without the
    // drop guard: the DELETE stayed applied (live=0), is_autocommit()
    // was left false, and the next fail() on that connection returned
    // Ok(0) — a silent miss from inside the leaked transaction, which is
    // the exact failure class this PR exists to close.
    #[test]
    fn in_savepoint_undoes_its_work_when_the_body_panics() {
        let conn = db();
        let id = claimed_job(&conn);

        let caught = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            in_savepoint::<()>(&conn, "honker_panic_probe", || {
                conn.execute("DELETE FROM _honker_live WHERE id = ?1", [id])
                    .unwrap();
                panic!("body panicked");
            })
        }));
        assert!(caught.is_err(), "the panic must still reach the caller");

        assert!(
            conn.is_autocommit(),
            "a panicking body must not leave the connection inside the \
             transaction in_savepoint opened"
        );
        assert_eq!(
            live_count(&conn, id),
            1,
            "a panicking body must not leave its DELETE applied"
        );
        assert!(
            conn.execute_batch("ROLLBACK TO SAVEPOINT honker_panic_probe")
                .is_err(),
            "the frame must be popped, not left on the savepoint stack"
        );
        // The connection has to still be usable, not silently inside a
        // transaction that swallows the next call.
        assert_eq!(
            fail(&conn, id, "w1", "after the panic").unwrap(),
            1,
            "the next call must act on the job, not return a silent miss \
             from inside a leaked transaction"
        );
        assert_eq!(dead_count(&conn, id), 1);
    }

    // Same thing on the path bindings take. rusqlite turns a panic
    // inside a scalar function into an "unwinding panic" error and hands
    // the connection back, so the connection outlives the panic and a
    // leaked frame would sit on it for every later call.
    #[test]
    fn a_panicking_savepoint_body_leaves_no_frame_on_the_scalar_path() {
        let conn = db();
        conn.create_scalar_function(
            "honker_test_panic_in_savepoint",
            0,
            rusqlite::functions::FunctionFlags::SQLITE_UTF8,
            |ctx| {
                let db = unsafe { ctx.get_connection() }?;
                in_savepoint::<i64>(&db, "honker_panic_probe_sql", || panic!("inside the udf"))
            },
        )
        .unwrap();

        let err = conn
            .query_row("SELECT honker_test_panic_in_savepoint()", [], |r| {
                r.get::<_, i64>(0)
            })
            .expect_err("a panicking body must surface as an error, not a value");
        assert!(
            err.to_string().contains("panic"),
            "expected the panic to be reported, got: {err}"
        );
        assert!(
            conn.is_autocommit(),
            "the caller's long-lived connection must not be left inside a \
             transaction the scalar function opened"
        );
        assert!(
            conn.execute_batch("ROLLBACK TO SAVEPOINT honker_panic_probe_sql")
                .is_err(),
            "no savepoint frame may be left on the caller's connection"
        );
    }

    // The `is_autocommit()` short-circuit in undo_savepoint_frame:
    // RAISE(ROLLBACK) makes SQLite roll the whole transaction back
    // itself, so there is no frame left to roll back to. The cause has
    // to reach the caller unchanged rather than wearing a bogus
    // "no such savepoint" clause.
    #[test]
    fn a_self_rolled_back_transaction_returns_the_cause_unchanged() {
        let conn = db();
        let id = claimed_job(&conn);
        conn.execute_batch(
            "CREATE TRIGGER test_rollback_dead BEFORE INSERT ON _honker_dead
             BEGIN SELECT RAISE(ROLLBACK, 'dead insert rolled back'); END",
        )
        .unwrap();

        let err = fail(&conn, id, "w1", "boom").expect_err("the ABORT must reach the caller");
        let msg = err.to_string();
        assert!(
            msg.contains("dead insert rolled back"),
            "the cause must reach the caller, got: {msg}"
        );
        assert!(
            !msg.contains("unknown state"),
            "SQLite had already rolled back, so nothing was left in an \
             unknown state; the error must not claim otherwise: {msg}"
        );
        assert!(conn.is_autocommit());
        assert_eq!(
            live_count(&conn, id),
            1,
            "the job must survive the rolled-back dead-letter move"
        );
        assert_eq!(dead_count(&conn, id), 0);
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

// `claimed_at` records when the CURRENT attempt started. The other
// timestamps cannot answer that: `created_at` includes queue wait,
// `run_at` is when the job became ready, and `claim_expires_at` moves
// on every heartbeat. These tests pin the four transitions that decide
// whether it means anything — first claim, heartbeat, retry, reclaim.
#[cfg(test)]
mod claimed_at_tests {
    use super::{get_job, heartbeat, retry};
    use crate::{attach_honker_functions, bootstrap_honker_schema};
    use rusqlite::Connection;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        conn
    }

    fn enqueue(conn: &Connection) -> i64 {
        conn.query_row(
            "SELECT honker_enqueue('emails', '{}', NULL, NULL, 0, 3, NULL)",
            [],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn claim(conn: &Connection, worker: &str, timeout_s: i64) -> String {
        conn.query_row(
            "SELECT honker_claim_batch('emails', ?1, 8, ?2)",
            rusqlite::params![worker, timeout_s],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn stored_claimed_at(conn: &Connection, id: i64) -> Option<i64> {
        conn.query_row(
            "SELECT claimed_at FROM _honker_live WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn now(conn: &Connection) -> i64 {
        conn.query_row("SELECT unixepoch()", [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn claimed_at_is_null_until_the_first_claim() {
        let conn = db();
        let id = enqueue(&conn);
        assert_eq!(
            stored_claimed_at(&conn, id),
            None,
            "a job that was never claimed must have claimed_at NULL, not 0 and not the enqueue time"
        );
        let job: serde_json::Value = serde_json::from_str(&get_job(&conn, id).unwrap()).unwrap();
        assert!(
            job["claimed_at"].is_null(),
            "get_job must report claimed_at as null on a pending job, got {}",
            job["claimed_at"]
        );
    }

    #[test]
    fn first_claim_sets_claimed_at_and_returns_it() {
        let conn = db();
        let id = enqueue(&conn);
        let before = now(&conn);
        let batch: serde_json::Value = serde_json::from_str(&claim(&conn, "w1", 300)).unwrap();
        let after = now(&conn);

        let claimed_at = batch[0]["claimed_at"]
            .as_i64()
            .expect("claim_batch must return claimed_at as a number");
        assert!(
            claimed_at >= before && claimed_at <= after,
            "claimed_at {claimed_at} must be the claim time, between {before} and {after}"
        );
        assert_eq!(stored_claimed_at(&conn, id), Some(claimed_at));

        let job: serde_json::Value = serde_json::from_str(&get_job(&conn, id).unwrap()).unwrap();
        assert_eq!(
            job["claimed_at"].as_i64(),
            Some(claimed_at),
            "get_job must report the same claimed_at the claim returned"
        );
    }

    // The reason the column exists. A heartbeat pushes claim_expires_at
    // out, so claim_expires_at cannot tell you how long the attempt has
    // been running. claimed_at has to stay put or it inherits the same
    // blind spot.
    #[test]
    fn heartbeat_moves_the_deadline_but_not_claimed_at() {
        let conn = db();
        let id = enqueue(&conn);
        claim(&conn, "w1", 300);
        // Backdate the claim. Without this the heartbeat lands in the
        // same second as the claim, so a heartbeat that DID refresh
        // claimed_at would write back the identical value and this test
        // would pass on broken code. Verified: with the backdate
        // removed, adding `claimed_at = unixepoch()` to heartbeat's
        // UPDATE still passed.
        conn.execute(
            "UPDATE _honker_live SET claimed_at = claimed_at - 3600 WHERE id = ?1",
            [id],
        )
        .unwrap();
        let claimed_at = stored_claimed_at(&conn, id).unwrap();
        let deadline_before: i64 = conn
            .query_row(
                "SELECT claim_expires_at FROM _honker_live WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();

        assert_eq!(heartbeat(&conn, id, "w1", 900).unwrap(), 1);

        let deadline_after: i64 = conn
            .query_row(
                "SELECT claim_expires_at FROM _honker_live WHERE id = ?1",
                [id],
                |r| r.get(0),
            )
            .unwrap();
        assert!(
            deadline_after > deadline_before,
            "the heartbeat must actually extend the deadline, \
             otherwise this test proves nothing about claimed_at"
        );
        assert_eq!(
            stored_claimed_at(&conn, id),
            Some(claimed_at),
            "a heartbeat must NOT refresh claimed_at: it extends the claim, \
             it does not start a new attempt"
        );
    }

    #[test]
    fn retry_clears_claimed_at_when_the_job_goes_back_to_pending() {
        let conn = db();
        let id = enqueue(&conn);
        claim(&conn, "w1", 300);
        assert!(stored_claimed_at(&conn, id).is_some());

        assert_eq!(retry(&conn, id, "w1", 0, "boom").unwrap(), 1);
        let state: String = conn
            .query_row("SELECT state FROM _honker_live WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(state, "pending", "retry must return the job to pending");
        assert_eq!(
            stored_claimed_at(&conn, id),
            None,
            "a job waiting in the queue is not running, so claimed_at must be cleared"
        );
    }

    // Reclaim after the visibility timeout is a new attempt, so
    // claimed_at moves. Otherwise it would report the first attempt's
    // start time forever.
    #[test]
    fn reclaim_resets_claimed_at_to_the_new_attempt() {
        let conn = db();
        let id = enqueue(&conn);
        // Claim with a timeout already in the past so the row is
        // immediately reclaimable.
        claim(&conn, "w1", -60);
        let first = stored_claimed_at(&conn, id).unwrap();
        // Backdate the first claim so the reclaim is unambiguously later.
        conn.execute(
            "UPDATE _honker_live SET claimed_at = claimed_at - 3600 WHERE id = ?1",
            [id],
        )
        .unwrap();

        // Read the clock before the reclaim. `second > first - 3600`
        // would only prove the value moved off the backdated one; any
        // write at all satisfies it. Measured: with the claim writing
        // `unixepoch() - 1000`, the old bound passes and this one
        // fails. `second >= before_reclaim` pins the actual reclaim.
        let before_reclaim = now(&conn);
        let batch: serde_json::Value = serde_json::from_str(&claim(&conn, "w2", 300)).unwrap();
        assert_eq!(batch[0]["id"].as_i64(), Some(id), "w2 must reclaim the row");
        let after_reclaim = now(&conn);

        let second = stored_claimed_at(&conn, id).unwrap();
        assert!(
            second >= before_reclaim && second <= after_reclaim,
            "a reclaim starts a new attempt, so claimed_at must be the reclaim time, \
             between {before_reclaim} and {after_reclaim}; got {second}, \
             backdated first claim was {}",
            first - 3600
        );
        assert_eq!(
            batch[0]["claimed_at"].as_i64(),
            Some(second),
            "the reclaim batch must report the new claimed_at"
        );
    }

    // Both terminal paths DELETE the row, so `claimed_at` leaves with
    // it. Neither reads the column. This pins that the extra column
    // breaks neither one, and that the row really leaves _honker_live
    // rather than lingering with a stale claimed_at.
    #[test]
    fn ack_and_fail_do_not_need_claimed_at_but_still_work() {
        let conn = db();

        let acked_id = enqueue(&conn);
        claim(&conn, "w1", 300);
        let acked: i64 = conn
            .query_row("SELECT honker_ack(?1, 'w1')", [acked_id], |r| r.get(0))
            .unwrap();
        assert_eq!(acked, 1, "the extra column must not break the ack path");
        assert_eq!(
            live_row_count(&conn, acked_id),
            0,
            "ack must remove the row, taking claimed_at with it"
        );

        let failed_id = enqueue(&conn);
        claim(&conn, "w1", 300);
        assert!(
            stored_claimed_at(&conn, failed_id).is_some(),
            "the claim must have set claimed_at before fail() runs"
        );
        let failed: i64 = conn
            .query_row("SELECT honker_fail(?1, 'w1', 'boom')", [failed_id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(failed, 1, "the extra column must not break the fail path");
        assert_eq!(
            live_row_count(&conn, failed_id),
            0,
            "fail must remove the row from _honker_live"
        );
        let dead: i64 = conn
            .query_row(
                "SELECT count(*) FROM _honker_dead WHERE id = ?1",
                [failed_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(dead, 1, "fail must land the job in _honker_dead");
    }

    // `claimed_at` is the start of the CURRENT claim, and it is only
    // meaningful while `claim_expires_at >= now`. When a claim lapses
    // with nobody reclaiming (attempts exhausted, no worker on the
    // queue, queue drained) the row keeps `state='processing'`,
    // `worker_id`, `claim_expires_at` AND `claimed_at` — all of it is
    // the last claim, and all of it goes stale together. Nothing
    // clears it; the next claim overwrites it. This is the one case
    // where `unixepoch() - claimed_at` read without the
    // `claim_expires_at` filter gives a growing number for an attempt
    // nobody is running, so the behaviour is pinned here and the
    // filter is documented in README.
    #[test]
    fn an_expired_claim_keeps_claimed_at_with_the_rest_of_the_stale_claim() {
        let conn = db();
        let id = enqueue(&conn);
        // A timeout already in the past: claimed, and immediately past
        // its deadline.
        claim(&conn, "w1", -60);

        let job: serde_json::Value = serde_json::from_str(&get_job(&conn, id).unwrap()).unwrap();
        let now_s = now(&conn);
        assert_eq!(job["state"].as_str(), Some("processing"));
        assert!(
            job["claim_expires_at"].as_i64().unwrap() < now_s,
            "the claim must actually be expired for this test to mean anything"
        );
        assert_eq!(
            job["worker_id"].as_str(),
            Some("w1"),
            "worker_id is not cleared on expiry either — claimed_at is no more \
             stale than the rest of the claim"
        );
        assert!(
            job["claimed_at"].as_i64().is_some(),
            "claimed_at is deliberately left in place: it is the last claim's \
             start, valid only while claim_expires_at >= now"
        );
    }

    fn live_row_count(conn: &Connection, id: i64) -> i64 {
        conn.query_row(
            "SELECT count(*) FROM _honker_live WHERE id = ?1",
            [id],
            |r| r.get(0),
        )
        .unwrap()
    }
}

// Queue-scoped cancel (issue #134). `honker_cancel` carries a global
// 1-arg form and a queue-scoped 2-arg form on the same connection while
// bindings migrate, so these tests cover both arities together: the
// scoped form must refuse a foreign queue without touching the row, and
// the global form must keep its current meaning exactly.
#[cfg(test)]
mod cancel_scoping {
    use crate::{
        CANCEL_QUEUE_SCOPED_PROBE_SQL, attach_honker_functions, bootstrap_honker_schema,
        has_queue_scoped_cancel,
    };
    use rusqlite::Connection;
    use rusqlite::OptionalExtension;
    use rusqlite::functions::FunctionFlags;

    fn db() -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        attach_honker_functions(&conn).unwrap();
        bootstrap_honker_schema(&conn).unwrap();
        conn
    }

    fn enqueue(conn: &Connection, queue: &str) -> i64 {
        conn.query_row(
            "SELECT honker_enqueue(?1, '{}', NULL, NULL, 0, 3, NULL)",
            [queue],
            |r| r.get(0),
        )
        .unwrap()
    }

    /// The queue a live row sits in, or `None` if the row is gone.
    /// Distinguishing "still there" from "deleted" is the whole point of
    /// the wrong-queue tests, so they assert on this, not just on the
    /// return count.
    fn live_queue(conn: &Connection, id: i64) -> Option<String> {
        // `.optional()` and not `.ok()`: `.ok()` turns EVERY error into
        // None, so a typo'd table name or a schema change would make
        // "the row is gone" assertions below pass without the cancel
        // having done anything. Only QueryReturnedNoRows means gone.
        conn.query_row("SELECT queue FROM _honker_live WHERE id = ?1", [id], |r| {
            r.get(0)
        })
        .optional()
        .unwrap()
    }

    /// Both arities are callable on the SAME connection, under the SAME
    /// name. This is the migration story: 1-arg callers keep working
    /// while bindings move to the 2-arg form one release at a time.
    #[test]
    fn both_arities_coexist_on_one_connection() {
        let conn = db();
        let a = enqueue(&conn, "emails");
        let b = enqueue(&conn, "sms");

        let one_arg: i64 = conn
            .query_row("SELECT honker_cancel(?1)", [a], |r| r.get(0))
            .unwrap();
        assert_eq!(one_arg, 1, "1-arg global cancel still resolves");

        let two_arg: i64 = conn
            .query_row("SELECT honker_cancel('sms', ?1)", [b], |r| r.get(0))
            .unwrap();
        assert_eq!(two_arg, 1, "2-arg scoped cancel resolves on the same conn");
    }

    /// The defect this closes: a handle for one queue must not delete
    /// another queue's row, and the row must survive the attempt.
    #[test]
    fn wrong_queue_cancels_nothing_and_leaves_the_row() {
        let conn = db();
        let sms_id = enqueue(&conn, "sms");

        let n: i64 = conn
            .query_row("SELECT honker_cancel('emails', ?1)", [sms_id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "emails handle must not cancel an sms job");
        assert_eq!(
            live_queue(&conn, sms_id).as_deref(),
            Some("sms"),
            "the sms row must still be live after a foreign-queue cancel"
        );

        let n: i64 = conn
            .query_row("SELECT honker_cancel('sms', ?1)", [sms_id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "the owning queue can cancel it");
        assert_eq!(live_queue(&conn, sms_id), None, "the row is gone");
    }

    /// Scoping must hold for a claimed (processing) row too, not just a
    /// pending one — that is the row a racing worker is holding, and the
    /// reason the queue check is inside the DELETE.
    #[test]
    fn scoping_holds_for_processing_rows() {
        let conn = db();
        let sms_id = enqueue(&conn, "sms");
        let _: String = conn
            .query_row("SELECT honker_claim_batch('sms', 'w1', 8, 300)", [], |r| {
                r.get(0)
            })
            .unwrap();
        let state: String = conn
            .query_row(
                "SELECT state FROM _honker_live WHERE id = ?1",
                [sms_id],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(state, "processing");

        let n: i64 = conn
            .query_row("SELECT honker_cancel('emails', ?1)", [sms_id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 0, "wrong queue must not cancel a processing row");
        assert_eq!(
            live_queue(&conn, sms_id).as_deref(),
            Some("sms"),
            "the processing sms row must still be live"
        );

        // The owning queue does reach the claimed row. This half is
        // what pins the 2-arg form to the same states as the 1-arg
        // form: without it, narrowing the scoped DELETE to
        // `state = 'pending'` leaves every other test in this module
        // passing, and the two arities silently disagree about
        // processing rows.
        let n: i64 = conn
            .query_row("SELECT honker_cancel('sms', ?1)", [sms_id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "the owning queue cancels a processing row");
        assert_eq!(
            live_queue(&conn, sms_id),
            None,
            "the processing row is gone after its own queue cancels it"
        );
    }

    /// The 1-arg form keeps its shipped meaning: global, ignores the
    /// queue. It becomes the documented `Database.cancel(id)` form. If
    /// this ever fails, the migration broke every existing caller.
    #[test]
    fn one_arg_form_is_still_global() {
        let conn = db();
        let sms_id = enqueue(&conn, "sms");
        let n: i64 = conn
            .query_row("SELECT honker_cancel(?1)", [sms_id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "1-arg cancel reaches a job in any queue");
        assert_eq!(live_queue(&conn, sms_id), None);
    }

    /// The 1-arg form's other shipped promise, the one its own doc
    /// comment makes: it removes a claimed (processing) row, not just a
    /// pending one. Nothing else in honker-core covered that, so the
    /// two arities could have drifted apart on state — one keeping
    /// 'processing' in its IN-list, the other losing it — with every
    /// other test here still green.
    #[test]
    fn one_arg_form_cancels_a_processing_row() {
        let conn = db();
        let id = enqueue(&conn, "sms");
        let _: String = conn
            .query_row("SELECT honker_claim_batch('sms', 'w1', 8, 300)", [], |r| {
                r.get(0)
            })
            .unwrap();
        let state: String = conn
            .query_row("SELECT state FROM _honker_live WHERE id = ?1", [id], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(state, "processing");

        let n: i64 = conn
            .query_row("SELECT honker_cancel(?1)", [id], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1, "1-arg cancel still reaches a claimed row");
        assert_eq!(live_queue(&conn, id), None);
    }

    /// Idempotence and the missing-id case at the new arity: a second
    /// cancel and an unknown id both return 0, same as the 1-arg form.
    #[test]
    fn repeat_and_missing_return_zero() {
        let conn = db();
        let id = enqueue(&conn, "emails");
        let first: i64 = conn
            .query_row("SELECT honker_cancel('emails', ?1)", [id], |r| r.get(0))
            .unwrap();
        let second: i64 = conn
            .query_row("SELECT honker_cancel('emails', ?1)", [id], |r| r.get(0))
            .unwrap();
        let missing: i64 = conn
            .query_row("SELECT honker_cancel('emails', 987654)", [], |r| r.get(0))
            .unwrap();
        assert_eq!((first, second, missing), (1, 0, 0));
    }

    /// SQLite dispatches on (name, arity), so a wrong argument count is
    /// a hard error rather than a silent fallback to the other form.
    /// That is exactly why the capability probe below has to exist.
    #[test]
    fn unregistered_arity_is_an_error() {
        let conn = db();
        let err = conn
            .query_row("SELECT honker_cancel('emails', 1, 2)", [], |r| {
                r.get::<_, i64>(0)
            })
            .unwrap_err();
        assert!(
            err.to_string().contains("wrong number of arguments"),
            "expected an arity error, got: {err}"
        );
    }

    /// A connection with the current functions attached reports the
    /// queue-scoped form as present, through both the exported SQL
    /// string and the Rust helper.
    #[test]
    fn probe_reports_true_with_the_two_arg_form() {
        let conn = db();
        assert!(
            has_queue_scoped_cancel(&conn).unwrap(),
            "helper must see honker_cancel/2 on a fully attached connection"
        );
        let raw: i64 = conn
            .query_row(CANCEL_QUEUE_SCOPED_PROBE_SQL, [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw, 1, "the exported probe SQL must return 1");
    }

    /// The case the probe exists for: an older vendored extension that
    /// registered only the global 1-arg `honker_cancel`. Registering
    /// arity 1 by hand reproduces that connection exactly — the name is
    /// present, the arity is not — and the probe must say false rather
    /// than being fooled by the name.
    #[test]
    fn probe_reports_false_without_the_two_arg_form() {
        let conn = Connection::open_in_memory().unwrap();
        conn.create_scalar_function("honker_cancel", 1, FunctionFlags::SQLITE_UTF8, |_ctx| {
            Ok(0i64)
        })
        .unwrap();

        let present: i64 = conn
            .query_row(
                "SELECT EXISTS(SELECT 1 FROM pragma_function_list \
                 WHERE name = 'honker_cancel' AND narg = 1)",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(present, 1, "precondition: the 1-arg form is registered");

        assert!(
            !has_queue_scoped_cancel(&conn).unwrap(),
            "helper must not report a queue-scoped cancel on an old extension"
        );
        let raw: i64 = conn
            .query_row(CANCEL_QUEUE_SCOPED_PROBE_SQL, [], |r| r.get(0))
            .unwrap();
        assert_eq!(raw, 0, "the exported probe SQL must return 0");
    }

    /// "Cannot tell" must not collapse into "absent". A real table
    /// named `pragma_function_list` shadows the eponymous pragma table
    /// on this connection, so the probe query no longer resolves —
    /// the same shape a SQLite built with
    /// SQLITE_OMIT_INTROSPECTION_PRAGMAS presents, where the pragma
    /// table is not there at all.
    ///
    /// This is the test for the claim the doc comment makes. Without
    /// it, `has_queue_scoped_cancel` could be rewritten to end in
    /// `.unwrap_or(false)` and every other test in this module would
    /// still pass — while a binding on a new extension got told the
    /// extension was old and took the wrong migration path.
    #[test]
    fn probe_error_is_not_flattened_to_false() {
        let conn = db();
        assert!(
            has_queue_scoped_cancel(&conn).unwrap(),
            "precondition: this connection answers the probe with true"
        );

        conn.execute_batch("CREATE TABLE pragma_function_list (x)")
            .unwrap();

        let err = has_queue_scoped_cancel(&conn)
            .expect_err("an unanswerable probe must be an Err, never Ok(false)");
        assert!(
            err.to_string().contains("no such column"),
            "expected the probe query itself to fail, got: {err}"
        );
    }
}
