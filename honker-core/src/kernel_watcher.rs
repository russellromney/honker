//! Optional kernel-watch backend (feature = `kernel-watcher`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for lower idle CPU and lower wake latency.
//!
//! # Contract
//!
//! `on_change()` fires on every relevant filesystem event observed on
//! the database file, its parent directory, or SQLite sidecar files
//! (`-wal`, `-shm`, `-journal`). **There is no `PRAGMA data_version`
//! verification and no safety-net poll.** This means:
//!
//! - **Spurious wakes are possible.** Any file change in the directory
//!   (other apps writing nearby files, the OS touching metadata, etc.)
//!   produces a wake. Consumers re-read state on every wake anyway, so
//!   this is wasted work, not incorrect.
//!
//! - **Missed wakes are possible.** If the OS drops or coalesces
//!   notifications, or fails to deliver an event for a SQLite commit,
//!   `on_change()` will not fire for that commit. The consumer's
//!   `idle_poll_s` (default 5 s) is the only backstop.
//!
//! - **Setup failures raise at `open()`.** [`probe`] runs at
//!   `honker.open()` time and surfaces any init failure as an error
//!   so the user knows immediately. No silent backend disable.
//!
//! Tests assert that wakes do fire, with bounded latency, on the
//! platforms we support. If a test fails, the backend is broken on
//! that platform — not "fall back to polling and pretend it worked".

use crate::stat_identity;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

#[cfg(target_os = "macos")]
use macos::{probe_kqueue, run_kqueue_loop};
#[cfg(not(target_os = "macos"))]
use notify::{EventKind, RecursiveMode, Watcher};
#[cfg(not(target_os = "macos"))]
use std::sync::mpsc;

/// How long `recv_timeout` blocks before sampling the stop flag.
/// Bounds graceful shutdown latency at this value.
const RX_POLL_MS: u64 = 50;
/// Cadence for the dead-man's switch (db-file replacement detection).
/// Same as the polling backend so file-replacement detection latency
/// doesn't depend on which backend the user picked.
const IDENTITY_CHECK_MS: u64 = 100;

pub(crate) fn run_kernel_watch_loop<F>(
    db_path: PathBuf,
    on_change: F,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::SyncSender<()>,
) where
    F: Fn() + Send + 'static,
{
    #[cfg(target_os = "macos")]
    {
        run_kqueue_loop(db_path, on_change, stop, ready);
        return;
    }

    #[cfg(not(target_os = "macos"))]
    {
        let (tx, rx) = mpsc::channel::<notify::Result<notify::Event>>();
        let mut watcher = match notify::recommended_watcher(tx) {
            Ok(w) => w,
            Err(e) => {
                eprintln!("honker: kernel-watcher init failed: {e}. Backend disabled.");
                return;
            }
        };

        // Attach watches: db file (catches in-place writes, the only signal
        // for non-WAL on macOS kqueue), parent dir (catches journal/wal/shm
        // create+delete in DELETE mode), and -wal/-shm/-journal directly
        // when present. No re-attach if files churn mid-flight — the
        // per-file watch goes stale and the consumer's `idle_poll_s`
        // backstop covers it. Experimental: restart to recover.
        let watch_dir = db_path
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        let wal = PathBuf::from(format!("{}-wal", db_path.display()));
        let shm = PathBuf::from(format!("{}-shm", db_path.display()));
        let journal = PathBuf::from(format!("{}-journal", db_path.display()));

        // Try each path; missing files / inaccessible dirs error here and
        // we just skip them. As long as at least one watch attached, we go.
        let attached = [&watch_dir, &db_path, &wal, &shm, &journal]
            .into_iter()
            .filter(|p| watcher.watch(p, RecursiveMode::NonRecursive).is_ok())
            .count();
        if attached == 0 {
            eprintln!(
                "honker: kernel-watcher couldn't attach to db dir or -wal/-shm. Backend disabled."
            );
            return;
        }

        // Dead-man's switch: snapshot db inode; panic if it changes
        // (atomic rename, litestream restore, NFS remount). Per-file
        // watches would silently sit on the dead inode otherwise.
        let initial_id = match stat_identity(&db_path) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("honker: failed to stat database for identity check: {e}");
                (0, 0)
            }
        };
        let mut last_id_check = Instant::now();
        // Baseline captured; signal the spawner that it's safe to return.
        let _ = ready.send(());
        drop(ready);

        while !stop.load(Ordering::Acquire) {
            match rx.recv_timeout(Duration::from_millis(RX_POLL_MS)) {
                Ok(Ok(event)) if !matches!(event.kind, EventKind::Access(_)) => on_change(),
                Ok(Err(e)) => {
                    // Notify error — fire conservatively so the consumer
                    // doesn't sit idle on a transient backend hiccup.
                    eprintln!("honker: kernel-watcher event error: {e}");
                    on_change();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                _ => {} // timeout, or Access event — ignore
            }
            if last_id_check.elapsed() >= Duration::from_millis(IDENTITY_CHECK_MS) {
                if check_db_identity(&db_path, initial_id) {
                    on_change();
                }
                last_id_check = Instant::now();
            }
        }
    }
}

/// Panics if the db file at `db_path` has been replaced since startup.
/// Returns `true` on stat error so caller can fire a conservative wake.
fn check_db_identity(db_path: &std::path::Path, initial: (u64, u64)) -> bool {
    match stat_identity(db_path) {
        Ok(current) => {
            if current != initial {
                panic!(
                    "honker: database file replaced: \
                     expected (dev={}, ino={}), found (dev={}, ino={}) at {:?}. \
                     The watcher cannot recover; \
                     close the Database and reopen with honker.open().",
                    initial.0, initial.1, current.0, current.1, db_path
                );
            }
            false
        }
        Err(e) => {
            eprintln!("honker: stat identity check failed: {e}");
            true
        }
    }
}

/// Probe at `honker.open()` so a misconfigured backend errors
/// immediately instead of silently producing no wakes.
pub(crate) fn probe(db_path: &std::path::Path) -> Result<(), String> {
    #[cfg(target_os = "macos")]
    {
        return probe_kqueue(db_path);
    }

    #[cfg(not(target_os = "macos"))]
    {
        let (tx, _rx) = mpsc::channel::<notify::Result<notify::Event>>();
        let mut w =
            notify::recommended_watcher(tx).map_err(|e| format!("notify init failed: {e}"))?;
        let dir = db_path.parent().unwrap_or(std::path::Path::new("."));
        w.watch(dir, RecursiveMode::NonRecursive)
            .map_err(|e| format!("can't watch {dir:?}: {e}"))?;
        // Drop the watcher; it's recreated when actually needed.
        Ok(())
    }
}

#[cfg(target_os = "macos")]
mod macos {
    use super::*;
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;
    use std::path::{Path, PathBuf};
    use std::ptr;

    struct Kqueue {
        fd: libc::c_int,
    }

    impl Kqueue {
        fn new() -> Result<Self, String> {
            let fd = unsafe { libc::kqueue() };
            if fd < 0 {
                return Err(format!(
                    "kqueue failed: {}",
                    std::io::Error::last_os_error()
                ));
            }
            Ok(Self { fd })
        }

        fn add_vnode(&self, fd: libc::c_int) -> Result<(), String> {
            let mut event = libc::kevent {
                ident: fd as libc::uintptr_t,
                filter: libc::EVFILT_VNODE,
                flags: libc::EV_ADD | libc::EV_ENABLE | libc::EV_CLEAR,
                fflags: libc::NOTE_WRITE
                    | libc::NOTE_EXTEND
                    | libc::NOTE_ATTRIB
                    | libc::NOTE_DELETE
                    | libc::NOTE_RENAME
                    | libc::NOTE_REVOKE,
                data: 0,
                udata: ptr::null_mut(),
            };
            let n =
                unsafe { libc::kevent(self.fd, &mut event, 1, ptr::null_mut(), 0, ptr::null()) };
            if n < 0 {
                Err(format!(
                    "kevent add failed: {}",
                    std::io::Error::last_os_error()
                ))
            } else {
                Ok(())
            }
        }

        fn wait_one(&self, timeout: Duration) -> Result<Option<libc::kevent>, String> {
            let mut event = libc::kevent {
                ident: 0,
                filter: 0,
                flags: 0,
                fflags: 0,
                data: 0,
                udata: ptr::null_mut(),
            };
            let ts = libc::timespec {
                tv_sec: timeout.as_secs() as libc::time_t,
                tv_nsec: i64::from(timeout.subsec_nanos()) as libc::c_long,
            };
            let n = unsafe { libc::kevent(self.fd, ptr::null(), 0, &mut event, 1, &ts) };
            if n < 0 {
                Err(format!(
                    "kevent wait failed: {}",
                    std::io::Error::last_os_error()
                ))
            } else if n == 0 {
                Ok(None)
            } else {
                Ok(Some(event))
            }
        }
    }

    impl Drop for Kqueue {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    struct WatchedPath {
        path: PathBuf,
        fd: libc::c_int,
    }

    impl Drop for WatchedPath {
        fn drop(&mut self) {
            unsafe {
                libc::close(self.fd);
            }
        }
    }

    fn open_event_fd(path: &Path) -> Result<libc::c_int, String> {
        let c_path = CString::new(path.as_os_str().as_bytes())
            .map_err(|_| format!("path contains NUL byte: {path:?}"))?;
        let fd = unsafe { libc::open(c_path.as_ptr(), libc::O_EVTONLY) };
        if fd < 0 {
            Err(format!(
                "open {path:?} failed: {}",
                std::io::Error::last_os_error()
            ))
        } else {
            Ok(fd)
        }
    }

    fn candidate_paths(db_path: &Path) -> Vec<PathBuf> {
        let mut paths = Vec::with_capacity(5);
        if let Some(parent) = db_path.parent() {
            paths.push(parent.to_path_buf());
        }
        paths.push(db_path.to_path_buf());
        paths.push(PathBuf::from(format!("{}-wal", db_path.display())));
        paths.push(PathBuf::from(format!("{}-shm", db_path.display())));
        paths.push(PathBuf::from(format!("{}-journal", db_path.display())));
        paths
    }

    fn attach_path(kq: &Kqueue, path: PathBuf, watched: &mut Vec<WatchedPath>) -> bool {
        if watched.iter().any(|w| w.path == path) {
            return false;
        }
        let fd = match open_event_fd(&path) {
            Ok(fd) => fd,
            Err(_) => return false,
        };
        if let Err(e) = kq.add_vnode(fd) {
            eprintln!("honker: kqueue couldn't watch {path:?}: {e}");
            unsafe {
                libc::close(fd);
            }
            return false;
        }
        watched.push(WatchedPath { path, fd });
        true
    }

    fn attach_existing(kq: &Kqueue, db_path: &Path, watched: &mut Vec<WatchedPath>) -> usize {
        candidate_paths(db_path)
            .into_iter()
            .filter(|path| path.exists())
            .filter(|path| attach_path(kq, path.clone(), watched))
            .count()
    }

    fn prune_deleted(event: &libc::kevent, watched: &mut Vec<WatchedPath>) {
        if event.fflags & (libc::NOTE_DELETE | libc::NOTE_RENAME | libc::NOTE_REVOKE) == 0 {
            return;
        }
        let ident = event.ident as libc::c_int;
        watched.retain(|w| w.fd != ident);
    }

    pub(super) fn run_kqueue_loop<F>(
        db_path: PathBuf,
        on_change: F,
        stop: Arc<AtomicBool>,
        ready: std::sync::mpsc::SyncSender<()>,
    ) where
        F: Fn() + Send + 'static,
    {
        let kq = match Kqueue::new() {
            Ok(kq) => kq,
            Err(e) => {
                eprintln!("honker: kernel-watcher init failed: {e}. Backend disabled.");
                return;
            }
        };
        let mut watched = Vec::new();
        let attached = attach_existing(&kq, &db_path, &mut watched);
        if attached == 0 {
            eprintln!(
                "honker: kqueue couldn't attach to db dir or database files. Backend disabled."
            );
            return;
        }

        let initial_id = match stat_identity(&db_path) {
            Ok(id) => id,
            Err(e) => {
                eprintln!("honker: failed to stat database for identity check: {e}");
                (0, 0)
            }
        };
        let mut last_id_check = Instant::now();
        let _ = ready.send(());
        drop(ready);

        while !stop.load(Ordering::Acquire) {
            match kq.wait_one(Duration::from_millis(RX_POLL_MS)) {
                Ok(Some(event)) => {
                    prune_deleted(&event, &mut watched);
                    let _ = attach_existing(&kq, &db_path, &mut watched);
                    on_change();
                }
                Ok(None) => {
                    let _ = attach_existing(&kq, &db_path, &mut watched);
                }
                Err(e) => {
                    eprintln!("honker: kqueue event error: {e}");
                    on_change();
                }
            }

            if last_id_check.elapsed() >= Duration::from_millis(IDENTITY_CHECK_MS) {
                if check_db_identity(&db_path, initial_id) {
                    on_change();
                }
                last_id_check = Instant::now();
            }
        }
    }

    pub(super) fn probe_kqueue(db_path: &Path) -> Result<(), String> {
        let kq = Kqueue::new()?;
        let dir = db_path.parent().unwrap_or(Path::new("."));
        let dir_fd = open_event_fd(dir)?;
        let result = kq.add_vnode(dir_fd);
        unsafe {
            libc::close(dir_fd);
        }
        result.map_err(|e| format!("can't watch {dir:?}: {e}"))
    }
}
