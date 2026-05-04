//! honker SQLite loadable extension.
//!
//! Thin wrapper around `honker-core`. Registers:
//!
//!   * `notify()` SQL scalar function + `_honker_notifications`
//!     table — via `honker_core::attach_notify`.
//!   * Every `honker_*` queue / lock / rate-limit / scheduler / result
//!     function — via `honker_core::attach_honker_functions`.
//!
//!     .load ./libhonker_ext
//!     SELECT honker_bootstrap();
//!     INSERT INTO _honker_live (queue, payload)
//!     VALUES ('emails', '{"to": "alice"}');
//!     SELECT honker_claim_batch('emails', 'worker-1', 32, 300);
//!     SELECT honker_ack_batch('[1,2,3]', 'worker-1');
//!     SELECT notify('orders', '{"id": 42}');
//!
//! Actual SQL implementations live in `honker_core::honker_ops`
//! so the Python (PyO3) and Node (napi-rs) bindings can register the
//! same functions on their own connections without loading this
//! `.dylib`. One source of truth for the SQL.

use rusqlite::ffi;
use rusqlite::functions::FunctionFlags;
use rusqlite::{Connection, Error, Result};
use std::collections::HashMap;
use std::ffi::CStr;
use std::os::raw::{c_char, c_int};
use std::panic::{AssertUnwindSafe, catch_unwind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex as StdMutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError};
use std::time::Duration;
use std::{ptr, sync::LazyLock};

fn panic_error(payload: Box<dyn std::any::Any + Send>) -> Error {
    let msg = if let Some(s) = payload.downcast_ref::<&str>() {
        *s
    } else if let Some(s) = payload.downcast_ref::<String>() {
        s.as_str()
    } else {
        "non-string panic payload"
    };
    Error::UserFunctionError(Box::new(std::io::Error::other(format!(
        "honker extension initialization panicked: {msg}"
    ))))
}

fn extension_init(conn: Connection) -> Result<bool> {
    match catch_unwind(AssertUnwindSafe(|| {
        honker_core::attach_notify(&conn).map_err(|e| {
            Error::UserFunctionError(Box::new(std::io::Error::other(e.to_string())))
        })?;
        honker_core::attach_honker_functions(&conn)?;
        attach_watcher_sql_functions(&conn)?;
        Ok(true)
    })) {
        Ok(result) => result,
        Err(payload) => Err(panic_error(payload)),
    }
}

static SQL_WATCHERS: LazyLock<StdMutex<HashMap<u64, HonkerWatcherHandle>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));
static NEXT_SQL_WATCHER_ID: AtomicU64 = AtomicU64::new(1);

/// Cache of SharedUpdateWatcher by (canonical-db-path, backend-name).
/// Keyed by Weak so a watcher whose last subscriber has gone away gets
/// dropped (and its background thread stopped). Subsequent opens on the
/// same path get a fresh watcher.
///
/// Without this cache every binding-side `honker_watcher_open` spawned a
/// fresh background poll thread + Arc + Mutex (~50-500 ms of setup).
/// Bindings that re-open the watcher per consumer (e.g. .NET creating a
/// new UpdatePoller per Queue.ClaimAsync) hit this cost on every async
/// call. With caching, the first open is normal-cost; subsequent opens
/// for the same (path, backend) attach a new subscriber to the existing
/// watcher in microseconds.
static SHARED_WATCHERS: LazyLock<StdMutex<HashMap<(String, String), std::sync::Weak<honker_core::SharedUpdateWatcher>>>> =
    LazyLock::new(|| StdMutex::new(HashMap::new()));

fn open_watcher_handle(
    db_path: &str,
    backend: Option<&str>,
) -> std::result::Result<HonkerWatcherHandle, String> {
    let backend_enum = honker_core::WatcherBackend::parse(backend.filter(|s| !s.is_empty()))?;
    backend_enum.probe(PathBuf::from(db_path).as_path())?;
    let backend_key = backend.unwrap_or("").to_string();
    let path_key = PathBuf::from(db_path)
        .canonicalize()
        .map(|p| p.to_string_lossy().into_owned())
        .unwrap_or_else(|_| db_path.to_string());
    let key = (path_key, backend_key);
    let shared = {
        let mut guard = SHARED_WATCHERS.lock().expect("SHARED_WATCHERS poisoned");
        if let Some(existing) = guard.get(&key).and_then(|w| w.upgrade()) {
            existing
        } else {
            let new_shared = Arc::new(honker_core::SharedUpdateWatcher::new_with_config(
                PathBuf::from(db_path),
                honker_core::WatcherConfig { backend: backend_enum },
            ));
            guard.insert(key, Arc::downgrade(&new_shared));
            new_shared
        }
    };
    let (sub_id, rx) = shared.subscribe();
    Ok(HonkerWatcherHandle { shared, sub_id, rx })
}

fn attach_watcher_sql_functions(conn: &Connection) -> Result<()> {
    conn.create_scalar_function(
        "honker_update_watcher_open",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let db_path: String = ctx.get(0)?;
            let backend: Option<String> = ctx.get(1)?;
            let handle = open_watcher_handle(&db_path, backend.as_deref()).map_err(|e| {
                rusqlite::Error::UserFunctionError(Box::new(std::io::Error::other(e)))
            })?;
            let id = NEXT_SQL_WATCHER_ID.fetch_add(1, Ordering::Relaxed);
            SQL_WATCHERS.lock().unwrap().insert(id, handle);
            Ok(id as i64)
        },
    )?;
    conn.create_scalar_function(
        "honker_update_watcher_wait",
        2,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let id: i64 = ctx.get(0)?;
            let timeout_ms: i64 = ctx.get(1)?;
            let Some(handle) = SQL_WATCHERS.lock().unwrap().remove(&(id as u64)) else {
                return Ok(-1);
            };
            let timeout_ms = timeout_ms.max(0) as u64;
            let code = match handle.rx.recv_timeout(Duration::from_millis(timeout_ms)) {
                Ok(()) => 1,
                Err(RecvTimeoutError::Timeout) => 0,
                Err(RecvTimeoutError::Disconnected) => -1,
            };
            if code != -1 {
                SQL_WATCHERS.lock().unwrap().insert(id as u64, handle);
            } else {
                handle.shared.unsubscribe(handle.sub_id);
                let _ = handle.shared.close();
            }
            Ok(code)
        },
    )?;
    conn.create_scalar_function(
        "honker_update_watcher_close",
        1,
        FunctionFlags::SQLITE_UTF8,
        |ctx| {
            let id: i64 = ctx.get(0)?;
            if let Some(handle) = SQL_WATCHERS.lock().unwrap().remove(&(id as u64)) {
                handle.shared.unsubscribe(handle.sub_id);
                let _ = handle.shared.close();
            }
            Ok(1)
        },
    )?;
    Ok(())
}

unsafe fn set_error_msg(
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
    message: &str,
) {
    if pz_err_msg.is_null() || p_api.is_null() {
        return;
    }
    let Some(malloc) = (unsafe { (*p_api).malloc }) else {
        return;
    };
    let len = match message.len().checked_add(1) {
        Some(len) if c_int::try_from(len).is_ok() => len,
        _ => return,
    };
    let ptr = unsafe { malloc(len as c_int) }.cast::<c_char>();
    if ptr.is_null() {
        return;
    }
    unsafe {
        ptr::copy_nonoverlapping(message.as_ptr().cast::<c_char>(), ptr, message.len());
        *ptr.add(message.len()) = 0;
        *pz_err_msg = ptr;
    }
}

unsafe fn extension_init2(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    if p_api.is_null() {
        return ffi::SQLITE_ERROR;
    }
    let result = unsafe { ffi::rusqlite_extension_init2(p_api) }
        .map_err(Error::from)
        .and_then(|()| unsafe { Connection::from_handle(db) })
        .and_then(extension_init);
    match result {
        Ok(true) => ffi::SQLITE_OK_LOAD_PERMANENTLY,
        Ok(false) => ffi::SQLITE_OK,
        Err(err) => {
            unsafe { set_error_msg(pz_err_msg, p_api, &err.to_string()) };
            ffi::SQLITE_ERROR
        }
    }
}

/// SQLite entry point. Name must match `sqlite3_<extname>_init`; SQLite
/// derives `<extname>` from the filename — stripping the `lib` prefix
/// and any non-alphabetic characters:
/// `libhonker_ext.dylib` -> `honker_ext` -> `honkerext`
/// -> `sqlite3_honkerext_init`.
///
/// # Safety
/// Called by SQLite. All pointers are SQLite-owned.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn sqlite3_honkerext_init(
    db: *mut ffi::sqlite3,
    pz_err_msg: *mut *mut c_char,
    p_api: *mut ffi::sqlite3_api_routines,
) -> c_int {
    match catch_unwind(AssertUnwindSafe(|| unsafe {
        extension_init2(db, pz_err_msg, p_api)
    })) {
        Ok(code) => code,
        Err(payload) => {
            let err = panic_error(payload);
            unsafe { set_error_msg(pz_err_msg, p_api, &err.to_string()) };
            ffi::SQLITE_ERROR
        }
    }
}

pub struct HonkerWatcherHandle {
    shared: Arc<honker_core::SharedUpdateWatcher>,
    sub_id: u64,
    rx: Receiver<()>,
}

unsafe fn cstr_to_string(ptr: *const c_char) -> std::result::Result<Option<String>, String> {
    if ptr.is_null() {
        return Ok(None);
    }
    let s = unsafe { CStr::from_ptr(ptr) }
        .to_str()
        .map_err(|e| format!("invalid UTF-8: {e}"))?;
    if s.is_empty() {
        Ok(None)
    } else {
        Ok(Some(s.to_string()))
    }
}

unsafe fn write_error(buf: *mut c_char, len: usize, message: &str) {
    if buf.is_null() || len == 0 {
        return;
    }
    let bytes = message.as_bytes();
    let copy_len = bytes.len().min(len.saturating_sub(1));
    unsafe {
        ptr::copy_nonoverlapping(bytes.as_ptr().cast::<c_char>(), buf, copy_len);
        *buf.add(copy_len) = 0;
    }
}

/// Open a core-backed update watcher over `db_path`.
///
/// Returns null on error and writes a NUL-terminated diagnostic into
/// `err_buf` when provided. `backend` accepts the same exact aliases as
/// `honker_core::WatcherBackend::parse`; null / empty means polling.
///
/// # Safety
/// All pointers must be valid NUL-terminated strings when non-null.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn honker_watcher_open(
    db_path: *const c_char,
    backend: *const c_char,
    err_buf: *mut c_char,
    err_buf_len: usize,
) -> *mut HonkerWatcherHandle {
    match catch_unwind(AssertUnwindSafe(|| {
        if db_path.is_null() {
            return Err("db_path is null".to_string());
        }
        let path = unsafe { CStr::from_ptr(db_path) }
            .to_str()
            .map_err(|e| format!("invalid db_path UTF-8: {e}"))?;
        let backend = unsafe { cstr_to_string(backend) }?;
        let handle = open_watcher_handle(path, backend.as_deref())?;
        Ok(Box::into_raw(Box::new(handle)))
    })) {
        Ok(Ok(ptr)) => ptr,
        Ok(Err(err)) => {
            unsafe { write_error(err_buf, err_buf_len, &err) };
            ptr::null_mut()
        }
        Err(payload) => {
            let err = panic_error(payload).to_string();
            unsafe { write_error(err_buf, err_buf_len, &err) };
            ptr::null_mut()
        }
    }
}

/// Wait for the next database update.
///
/// Returns:
/// * `1` when an update was observed
/// * `0` on timeout
/// * `-1` when the watcher/subscription has closed or died
/// * `-2` if this function catches an internal panic
///
/// # Safety
/// `handle` must be a pointer returned by `honker_watcher_open` and not
/// yet passed to `honker_watcher_close`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn honker_watcher_wait(
    handle: *mut HonkerWatcherHandle,
    timeout_ms: u64,
) -> c_int {
    if handle.is_null() {
        return -1;
    }
    match catch_unwind(AssertUnwindSafe(|| {
        let handle = unsafe { &mut *handle };
        match handle.rx.recv_timeout(Duration::from_millis(timeout_ms)) {
            Ok(()) => 1,
            Err(RecvTimeoutError::Timeout) => 0,
            Err(RecvTimeoutError::Disconnected) => -1,
        }
    })) {
        Ok(code) => code,
        Err(_) => -2,
    }
}

/// Close a watcher opened by `honker_watcher_open`.
///
/// # Safety
/// `handle` must be null or a pointer returned by `honker_watcher_open`.
/// Passing the same non-null pointer twice is undefined behavior.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn honker_watcher_close(handle: *mut HonkerWatcherHandle) {
    if handle.is_null() {
        return;
    }
    let handle = unsafe { Box::from_raw(handle) };
    handle.shared.unsubscribe(handle.sub_id);
    let _ = handle.shared.close();
}
