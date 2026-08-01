//! Optional kernel-watch backend (feature = `kernel-watcher`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for lower idle CPU and lower wake latency.
//!
//! # Contract
//!
//! `on_change()` fires on every relevant filesystem event observed on
//! the database's parent directory and its rollback/WAL sidecar files.
//! **There is no `PRAGMA data_version` verification and no safety-net
//! poll.** This means:
//!
//! Which paths are watched is a *safety* decision, not just a coverage
//! one: any backend that holds a descriptor per watched file must never
//! open the main database file or `-shm`, because closing that descriptor
//! releases the whole process's SQLite POSIX locks on it. See
//! [`macos::candidate_paths`] and issue #80.
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
use notify::{RecursiveMode, Watcher};
#[cfg(not(target_os = "macos"))]
use std::collections::HashSet;
#[cfg(not(target_os = "macos"))]
use std::path::Path;
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

        // Attach watches: db file (catches in-place writes), parent dir
        // (catches journal/wal/shm create+delete), and sidecars directly
        // when present. SQLite can create the WAL after watcher startup,
        // so retry per-file attaches on parent-dir activity and on the
        // dead-man cadence.
        let watch_dir = db_path
            .parent()
            .unwrap_or(std::path::Path::new("."))
            .to_path_buf();
        let wal = PathBuf::from(format!("{}-wal", db_path.display()));
        let shm = PathBuf::from(format!("{}-shm", db_path.display()));
        let journal = PathBuf::from(format!("{}-journal", db_path.display()));

        let targets = notify_watch_targets(&watch_dir, &db_path, &wal, &shm, &journal);

        let mut watched = HashSet::new();
        let mut attached = 0;
        for path in &targets {
            if attach_watch(&mut watcher, &mut watched, path) {
                attached += 1;
            }
        }
        if attached == 0 {
            eprintln!(
                "honker: kernel-watcher couldn't attach to db dir or -wal/-journal. Backend disabled."
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
        let _ = ready.send(());
        drop(ready);

        while !stop.load(Ordering::Acquire) {
            match rx.recv_timeout(Duration::from_millis(RX_POLL_MS)) {
                Ok(Ok(_event)) => {
                    for path in &targets {
                        let _ = attach_watch(&mut watcher, &mut watched, path);
                    }
                    on_change();
                }
                Ok(Err(e)) => {
                    eprintln!("honker: kernel-watcher event error: {e}");
                    on_change();
                }
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
                _ => {}
            }
            if last_id_check.elapsed() >= Duration::from_millis(IDENTITY_CHECK_MS) {
                for path in &targets {
                    let _ = attach_watch(&mut watcher, &mut watched, path);
                }
                if check_db_identity(&db_path, initial_id) {
                    on_change();
                }
                last_id_check = Instant::now();
            }
        }
    }
}

/// Paths the `notify` backend attaches watches to.
///
/// Which files are safe depends on whether the platform's `notify` backend
/// keeps a descriptor open per watched file:
///
/// * **inotify** (Linux/Android) registers a watch by path on the single
///   inotify instance fd — it never holds a descriptor on the watched file,
///   so nothing this crate does can release a SQLite lock. The database
///   file and `-shm` stay in the set; dropping them would cost detection in
///   TRUNCATE/PERSIST journal modes for no safety gain.
/// * **`ReadDirectoryChangesW`** (Windows) watches directories and Windows
///   does not use POSIX advisory locks at all.
/// * **kqueue** (the BSDs) holds one `O_EVTONLY` descriptor per watched
///   file and closes it on unwatch/drop — the same hazard as the hand-rolled
///   macOS backend below. The database file and `-shm` are excluded there.
///   See [`macos::candidate_paths`] for the full explanation and issue #80.
#[cfg(not(target_os = "macos"))]
fn notify_watch_targets(
    watch_dir: &Path,
    db_path: &Path,
    wal: &Path,
    shm: &Path,
    journal: &Path,
) -> Vec<PathBuf> {
    // notify::RecommendedWatcher == KqueueWatcher on these targets.
    #[cfg(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "ios"
    ))]
    {
        let _ = (db_path, shm);
        vec![
            watch_dir.to_path_buf(),
            wal.to_path_buf(),
            journal.to_path_buf(),
        ]
    }
    #[cfg(not(any(
        target_os = "freebsd",
        target_os = "openbsd",
        target_os = "netbsd",
        target_os = "dragonfly",
        target_os = "ios"
    )))]
    {
        vec![
            watch_dir.to_path_buf(),
            db_path.to_path_buf(),
            wal.to_path_buf(),
            shm.to_path_buf(),
            journal.to_path_buf(),
        ]
    }
}

#[cfg(not(target_os = "macos"))]
fn attach_watch<W: Watcher>(watcher: &mut W, watched: &mut HashSet<PathBuf>, path: &Path) -> bool {
    if watched.contains(path) {
        return false;
    }
    if watcher.watch(path, RecursiveMode::NonRecursive).is_ok() {
        watched.insert(path.to_path_buf());
        return true;
    }
    false
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
        probe_kqueue(db_path)
    }

    #[cfg(not(target_os = "macos"))]
    {
        let (tx, _rx) = mpsc::channel::<notify::Result<notify::Event>>();
        let mut w =
            notify::recommended_watcher(tx).map_err(|e| format!("notify init failed: {e}"))?;
        let dir = db_path.parent().unwrap_or(std::path::Path::new("."));
        w.watch(dir, RecursiveMode::NonRecursive)
            .map_err(|e| format!("can't watch {dir:?}: {e}"))?;
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
            let event = libc::kevent {
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
            let n = unsafe { libc::kevent(self.fd, &event, 1, ptr::null_mut(), 0, ptr::null()) };
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

    /// The db's parent directory, normalized. `Path::parent()` returns
    /// `Some("")` for a bare relative filename like `"a.db"`, and
    /// `open("")` fails with ENOENT — so map that to `"."`.
    pub(super) fn watch_dir(db_path: &Path) -> PathBuf {
        match db_path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p.to_path_buf(),
            _ => PathBuf::from("."),
        }
    }

    /// Paths kqueue may hold an open descriptor on.
    ///
    /// # LOCK-BEARING FILES MUST NEVER APPEAR HERE
    ///
    /// **Do not add the main database file or `-shm` to this list.**
    /// kqueue's `EVFILT_VNODE` requires a descriptor per watched file, and
    /// those descriptors get closed — on `NOTE_DELETE` pruning, on attach
    /// failure, and at watcher shutdown. On POSIX, `close()` of *any*
    /// descriptor for an inode releases *every* advisory lock the calling
    /// process holds on that inode, including locks taken by unrelated
    /// SQLite connections living in the same process. SQLite's
    /// `unixInodeInfo` deferred-close list (`setPendingFd` in `os_unix.c`)
    /// only defers closes of descriptors SQLite itself opened; it cannot
    /// see ours.
    ///
    /// In WAL mode SQLite takes POSIX locks on exactly two files: the main
    /// database file (a SHARED lock held for the connection's lifetime) and
    /// `-shm` (the WAL-index locks plus the DMS byte). Dropping either one
    /// lets another process delete `-wal`/`-shm` out from under a live
    /// connection — whose next commit then lands in an unlinked WAL and is
    /// silently lost — or re-run WAL-index recovery and `ftruncate` `-shm`
    /// under a live mapping, which is SIGBUS. See issue #80.
    ///
    /// `-wal` and `-journal` are safe to watch: `os_unix.c` sets
    /// `UNIXFILE_NOLOCK` for every file whose open type is not
    /// `SQLITE_OPEN_MAIN_DB`, so they use `nolockIoMethods` and never carry
    /// a lock. Directories are safe: SQLite opens them only to `fsync`.
    ///
    /// Detection is not weakened in WAL mode: every commit appends frames
    /// to `-wal`, which is exactly what raises `NOTE_WRITE`/`NOTE_EXTEND`.
    /// Rollback modes signal through `-journal` (DELETE unlinks it,
    /// TRUNCATE truncates it, PERSIST zeroes its header) and through the
    /// parent directory.
    pub(super) fn candidate_paths(db_path: &Path) -> Vec<PathBuf> {
        vec![
            watch_dir(db_path),
            PathBuf::from(format!("{}-wal", db_path.display())),
            PathBuf::from(format!("{}-journal", db_path.display())),
        ]
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
                "honker: kqueue couldn't attach to db dir or -wal/-journal. Backend disabled."
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

    /// Probe only the parent directory. Directories carry no SQLite locks,
    /// so the `close()` below cannot release any — see [`candidate_paths`].
    /// Probing the database file or `-shm` here would drop this process's
    /// SQLite locks on every `honker.open()`.
    pub(super) fn probe_kqueue(db_path: &Path) -> Result<(), String> {
        let kq = Kqueue::new()?;
        let dir = watch_dir(db_path);
        let dir_fd = open_event_fd(&dir)?;
        let result = kq.add_vnode(dir_fd);
        unsafe {
            libc::close(dir_fd);
        }
        result.map_err(|e| format!("can't watch {dir:?}: {e}"))
    }
}

#[cfg(test)]
mod tests {
    #[cfg(target_os = "macos")]
    use std::path::{Path, PathBuf};

    /// Structural guard for issue #80. The kqueue backend holds one
    /// descriptor per watched path and closes it on prune, on attach
    /// failure, and at shutdown; on POSIX that close releases every
    /// advisory lock this process holds on the inode. SQLite locks the
    /// main database file and `-shm`, so neither may ever be watched.
    ///
    /// The behavioral proof lives in
    /// `lib.rs::tests::kernel_watcher_does_not_release_sqlite_wal_locks`.
    /// This test is the cheap, platform-independent tripwire that fires
    /// the moment someone adds a path back to the list.
    #[test]
    #[cfg(target_os = "macos")]
    fn candidate_paths_excludes_every_file_sqlite_locks() {
        let db = Path::new("/tmp/honker-cp/app.db");
        let paths = super::macos::candidate_paths(db);

        assert!(
            !paths.contains(&db.to_path_buf()),
            "the main database file carries SQLite's SHARED lock and must \
             never be kqueue-watched: {paths:?}"
        );
        assert!(
            !paths.contains(&PathBuf::from("/tmp/honker-cp/app.db-shm")),
            "-shm carries SQLite's WAL-index and DMS locks and must never \
             be kqueue-watched: {paths:?}"
        );
        // And the wake signal we depend on in WAL mode is still there.
        assert!(
            paths.contains(&PathBuf::from("/tmp/honker-cp/app.db-wal")),
            "-wal is the WAL-mode commit signal and must stay watched: {paths:?}"
        );
        assert!(
            paths.contains(&PathBuf::from("/tmp/honker-cp")),
            "the parent directory must stay watched: {paths:?}"
        );
    }

    /// `Path::parent()` yields `Some("")` for a bare relative filename,
    /// and `open("")` is ENOENT. The directory watch is load-bearing now
    /// that the database file itself is not watched, so this must resolve
    /// to `"."` rather than silently failing to attach.
    #[test]
    #[cfg(target_os = "macos")]
    fn bare_relative_db_path_watches_the_current_directory() {
        assert_eq!(
            super::macos::watch_dir(Path::new("app.db")),
            PathBuf::from(".")
        );
        assert_eq!(
            super::macos::watch_dir(Path::new("/var/db/app.db")),
            PathBuf::from("/var/db")
        );
    }
}
