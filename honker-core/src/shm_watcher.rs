//! Optional `-shm` fast path (feature = `shm-fast-path`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for slightly lower CPU per tick. Not lower
//! latency — see "What this backend is actually worth" below.
//!
//! # Contract
//!
//! `on_change()` fires when the `iChange` counter at byte offset 8 of
//! the WAL index header (`-shm` file) advances. **There is no `PRAGMA
//! data_version` verification, no safety-net poll, no inode re-mmap.**
//! This means:
//!
//! - **WAL mode required.** No `-shm` exists in DELETE/TRUNCATE/
//!   PERSIST modes. If the file isn't present at startup the backend
//!   logs to stderr and exits — no wakes ever fire.
//!
//! - **Trusts the on-disk shm layout.** Reads `iChange` at a fixed
//!   offset and assumes it tracks `PRAGMA data_version`. Verified by
//!   the equivalence test (`shm_fast_path_equivalence_with_pragma_baseline`)
//!   on every supported SQLite version. If a future SQLite version
//!   changes the layout, this breaks silently.
//!
//! - **WAL reset / db replacement: watcher death.** If `-shm` or the db
//!   file is deleted and recreated mid-flight (cross-process close+reopen,
//!   atomic rename, litestream restore), the watcher panics with a
//!   "Restart required" message. Same dead-man's-switch shape as the
//!   polling backend — louder failure than silent missed wakes. The file
//!   is read with bounded positional reads instead of mmap so SQLite file
//!   churn cannot SIGBUS the host process.
//!
//! - **`-shm` descriptors are opened once and never closed early.** SQLite
//!   locks `-shm`, and on POSIX closing any descriptor for an inode drops
//!   every lock the *process* holds on it. See the registry below and
//!   issue #80. This is why the backend does not simply `File::open` where
//!   it needs a read.
//!
//! # What this backend is actually worth
//!
//! Less than its name suggests, and you should know that before enabling
//! it. The original design mapped `-shm` and read `iChange` as a load,
//! which was ~2000x cheaper than `PRAGMA data_version`. That mapping had
//! to go: SQLite truncates `-shm` to 3 bytes when it re-attaches the WAL
//! index, and a mapped read past the new end of file is SIGBUS, which
//! kills the host process. Bounded positional reads replaced it.
//!
//! A bounded read is a syscall. Measured on macOS/arm64 against SQLite
//! 3.51.3: `pread` of the header ~1.2 us, `PRAGMA data_version` ~2.3 us.
//! Both backends then sleep 1 ms, so **wake latency is identical** — the
//! only difference is roughly 1 us of CPU per millisecond per watched
//! database, about 0.1% of a core. Prefer the polling backend unless you
//! have measured a reason not to.
//!
//! Tests assert that wakes fire with sub-millisecond latency in WAL
//! mode. If a test fails, the backend is broken — not "fall back to
//! polling and pretend it worked".

use crate::stat_identity;
use rusqlite::{Connection, OpenFlags};
use std::collections::HashMap;
use std::fs::File;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

const WALINDEX_MAX_VERSION: u32 = 3_007_000;
const ICHANGE_OFFSET: usize = 8;
/// Same cadence as the polling backend. The win over polling is CPU per
/// tick, not latency — both sleep 1 ms, so wake latency is identical.
/// Measured on macOS/arm64 against SQLite 3.51.3: `pread` of the header
/// is ~1.2 us versus ~2.3 us for `PRAGMA data_version`. That is a much
/// smaller margin than the original mmap design promised, because a
/// bounded read is a syscall and a mapped load was not. See the module
/// docs for why the mapping had to go.
const POLL_INTERVAL_MS: u64 = 1;
/// Cadence for the dead-man's switch (db / -shm replacement detection).
/// Same wall-clock interval as the polling and kernel backends. Tracked
/// via Instant — tick counting drifts on Windows where 1 ms sleeps round
/// up to ~15 ms.
const IDENTITY_CHECK_INTERVAL: Duration = Duration::from_millis(100);

// ---------------------------------------------------------------------
// `-shm` descriptor registry (issue #80)
// ---------------------------------------------------------------------
//
// SQLite takes POSIX advisory locks on `-shm` (the WAL-index locks and
// the DMS byte). On POSIX, `close()` of *any* descriptor for an inode
// releases *every* lock the calling process holds on it — including
// locks belonging to unrelated SQLite connections in the same process.
// SQLite's `unixInodeInfo` deferred-close list only defers closes of
// descriptors SQLite itself opened, so it cannot protect us.
//
// So this module must never close a `-shm` descriptor while a connection
// in this process might still hold locks on that inode. Three call sites
// used to do exactly that: `probe()` on every `honker.open()`, the
// identity-change reopen, and the watcher's own shutdown.
//
// The rule enforced here:
//
//   * open each `-shm` inode at most once per process, and share it;
//   * never close a descriptor whose inode still has a name;
//   * reclaim descriptors once `st_nlink == 0`, which proves the inode is
//     unreferenced. SQLite only unlinks `-shm` while holding an EXCLUSIVE
//     lock on the database file — i.e. when no connection anywhere is
//     using that WAL — so at that point there is no lock left to drop.
//
// That bounds retention to one descriptor per *live* watched database
// rather than leaking one per WAL generation.

/// How many `-shm` descriptors this process will retain before giving up.
/// Only reachable if inodes are churning faster than they can be reclaimed;
/// failing loudly beats leaking descriptors without limit.
#[cfg(unix)]
const MAX_RETAINED_SHM_FDS: usize = 256;

#[cfg(unix)]
#[derive(Default)]
struct ShmFdRegistry {
    /// Current descriptor per `(dev, ino)`, shared by every watcher on it.
    live: HashMap<(u64, u64), Arc<File>>,
    /// Descriptors we could not file under a free key (lost an open race).
    /// Held, not closed, until reclaimable.
    extra: Vec<Arc<File>>,
}

#[cfg(unix)]
fn shm_registry() -> &'static Mutex<ShmFdRegistry> {
    static REGISTRY: OnceLock<Mutex<ShmFdRegistry>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(ShmFdRegistry::default()))
}

/// `st_nlink` for an already-open file. `0` means the inode has no
/// remaining directory entry, so nothing can lock it any more.
#[cfg(unix)]
fn link_count(file: &File) -> Option<u64> {
    use std::os::unix::fs::MetadataExt;
    file.metadata().ok().map(|m| m.nlink())
}

#[cfg(unix)]
impl ShmFdRegistry {
    /// Drop every descriptor whose inode is provably unreferenced and that
    /// no watcher still holds. Cheap: one `fstat` per retained descriptor,
    /// run only on acquire.
    fn reclaim_unlinked(&mut self) {
        self.live
            .retain(|_, fd| Arc::strong_count(fd) > 1 || link_count(fd) != Some(0));
        self.extra
            .retain(|fd| Arc::strong_count(fd) > 1 || link_count(fd) != Some(0));
    }

    fn retained(&self) -> usize {
        self.live.len() + self.extra.len()
    }
}

/// Get a shared descriptor for `path`, opening it only if this process
/// does not already hold one for that inode.
///
/// The `stat`-before-`open` step is not an optimization — it is the whole
/// point. Opening a second descriptor just to discover we already had one
/// would mean closing it, and that close is the bug.
#[cfg(unix)]
fn acquire_shm_fd(path: &Path) -> std::io::Result<Arc<File>> {
    let existing_id = stat_identity(path).ok();
    let mut reg = shm_registry().lock().unwrap_or_else(|e| e.into_inner());
    reg.reclaim_unlinked();

    if let Some(id) = existing_id
        && let Some(fd) = reg.live.get(&id)
    {
        return Ok(Arc::clone(fd));
    }

    if reg.retained() >= MAX_RETAINED_SHM_FDS {
        return Err(std::io::Error::other(format!(
            "shm-fast-path is holding {MAX_RETAINED_SHM_FDS} -shm descriptors it \
             cannot safely close; refusing to open more. Closing them would \
             release this process's SQLite WAL-index locks (issue #80)."
        )));
    }

    let file = Arc::new(File::open(path)?);
    // Re-identify from the descriptor: `path` may have been replaced
    // between the stat above and this open.
    let id = match stat_identity_of(&file) {
        Some(id) => id,
        None => {
            reg.extra.push(Arc::clone(&file));
            return Ok(file);
        }
    };
    match reg.live.entry(id) {
        std::collections::hash_map::Entry::Occupied(slot) => {
            // Raced with another thread. Keep ours alive rather than
            // closing it, and hand back the descriptor already on file.
            let winner = Arc::clone(slot.get());
            reg.extra.push(file);
            Ok(winner)
        }
        std::collections::hash_map::Entry::Vacant(slot) => {
            slot.insert(Arc::clone(&file));
            Ok(file)
        }
    }
}

/// `(dev, ino)` of an open file, matching [`crate::stat_identity`]'s shape.
#[cfg(unix)]
fn stat_identity_of(file: &File) -> Option<(u64, u64)> {
    use std::os::unix::fs::MetadataExt;
    let m = file.metadata().ok()?;
    Some((m.dev(), m.ino()))
}

/// Windows has no POSIX advisory locks — closing a handle cannot release
/// another connection's lock — so the retention machinery above is not
/// needed and would only keep handles alive on a live `-shm`.
#[cfg(not(unix))]
fn acquire_shm_fd(path: &Path) -> std::io::Result<Arc<File>> {
    File::open(path).map(Arc::new)
}

/// A `-shm` descriptor, the 12-byte WAL-index header read from it, and
/// that file's `(dev, ino)` at the time of the read.
type ShmHeaderSnapshot = (Arc<File>, [u8; 12], (u64, u64));

/// Positional read of the WAL-index header. One syscall, and — unlike a
/// mapping — a file that shrinks underneath us yields `UnexpectedEof`
/// instead of SIGBUS. That protection came from PR #43 and must stay.
fn read_wal_index_header(file: &File) -> std::io::Result<[u8; 12]> {
    let mut header = [0_u8; 12];
    #[cfg(unix)]
    {
        use std::os::unix::fs::FileExt;
        file.read_exact_at(&mut header, 0)?;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::FileExt;
        let mut done = 0;
        while done < header.len() {
            match file.seek_read(&mut header[done..], done as u64)? {
                0 => {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        "-shm shorter than the WAL index header",
                    ));
                }
                n => done += n,
            }
        }
    }
    Ok(header)
}

pub(crate) fn run_shm_fast_path_loop<F>(
    db_path: PathBuf,
    on_change: F,
    stop: Arc<AtomicBool>,
    ready: std::sync::mpsc::SyncSender<()>,
) where
    F: Fn() + Send + 'static,
{
    if cfg!(target_endian = "big") {
        eprintln!("honker: shm-fast-path requires little-endian platform. Backend disabled.");
        return;
    }
    let shm_path = PathBuf::from(format!("{}-shm", db_path.display()));
    // Keep a quiet SQLite read connection open for the lifetime of the
    // watcher so normal cross-process open/close churn does not reap or
    // truncate the WAL-index file underneath the fast path. Do not apply
    // the default PRAGMAs here: the application connection already set
    // WAL mode before the watcher is opened, and this connection should
    // not participate in journal-mode setup or checkpoints.
    let _keeper = match Connection::open_with_flags(
        &db_path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    ) {
        Ok(conn) => Some(conn),
        Err(e) => {
            eprintln!("honker: shm-fast-path keeper connection failed: {e}");
            None
        }
    };
    let (mut f, header, mut initial_shm_id) = match wait_for_initial_shm_header(&shm_path, &stop) {
        Some(parts) => parts,
        None => {
            let _ = ready.send(());
            return;
        }
    };
    // Sanity: WAL index version we know how to read. A future SQLite
    // that bumps this fails the check instead of reading garbage.
    let iversion = u32::from_ne_bytes(header[0..4].try_into().unwrap());
    if iversion != WALINDEX_MAX_VERSION {
        eprintln!(
            "honker: shm-fast-path disabled: WAL index version {iversion} != {WALINDEX_MAX_VERSION}."
        );
        return;
    }

    let mut last = read_ichange_from_header(&header);

    // Dead-man's switch: snapshot db + -shm inodes; panic on change.
    // Without this the mmap silently sits on a dead -shm inode.
    let initial_db_id = match stat_identity(&db_path) {
        Ok(id) => id,
        Err(e) => {
            eprintln!("honker: failed to stat database for identity check: {e}");
            (0, 0)
        }
    };
    let mut next_identity_check = Instant::now() + IDENTITY_CHECK_INTERVAL;
    // Baseline captured; signal the spawner that it's safe to return.
    let _ = ready.send(());
    drop(ready);

    while !stop.load(Ordering::Acquire) {
        std::thread::sleep(Duration::from_millis(POLL_INTERVAL_MS));
        let current = match read_wal_index_header(&f) {
            Ok(header) => read_ichange_from_header(&header),
            Err(e) => {
                if let Some((new_file, new_header, new_id)) = reopen_shm_header(&shm_path) {
                    f = new_file;
                    initial_shm_id = new_id;
                    last = read_ichange_from_header(&new_header);
                    on_change();
                    continue;
                }
                eprintln!("honker: shm-fast-path read failed: {e}");
                on_change();
                std::thread::sleep(Duration::from_millis(10));
                continue;
            }
        };
        if current != last {
            last = current;
            on_change();
        }
        let now = Instant::now();
        if now >= next_identity_check {
            next_identity_check = now + IDENTITY_CHECK_INTERVAL;
            let db_stat_err = check_db_identity(&db_path, initial_db_id);
            match stat_identity(&shm_path) {
                Ok(current_id) if current_id != initial_shm_id => {
                    if let Some((new_file, new_header, new_id)) = reopen_shm_header(&shm_path) {
                        f = new_file;
                        initial_shm_id = new_id;
                        last = read_ichange_from_header(&new_header);
                    }
                    on_change();
                }
                Ok(_) => {}
                Err(e) => {
                    eprintln!("honker: stat identity check failed for -shm file: {e}");
                    on_change();
                }
            }
            if db_stat_err {
                on_change();
            }
        }
    }
}

fn read_ichange_from_header(header: &[u8; 12]) -> u32 {
    u32::from_ne_bytes(
        header[ICHANGE_OFFSET..ICHANGE_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

/// Re-acquire the current `-shm` descriptor and re-read its header.
///
/// Goes through [`acquire_shm_fd`], so the descriptor this replaces is
/// retained by the registry rather than closed. Closing it here would
/// release the WAL-index locks of every SQLite connection in this
/// process — the identity-change path was one of the three sites that
/// did exactly that before issue #80.
fn reopen_shm_header(path: &std::path::Path) -> Option<ShmHeaderSnapshot> {
    let file = acquire_shm_fd(path).ok()?;
    let header = read_wal_index_header(&file).ok()?;
    let id = stat_identity(path).ok()?;
    Some((file, header, id))
}

fn wait_for_initial_shm_header(
    path: &std::path::Path,
    stop: &AtomicBool,
) -> Option<ShmHeaderSnapshot> {
    for _ in 0..200 {
        if stop.load(Ordering::Acquire) {
            return None;
        }
        if let Some(parts) = reopen_shm_header(path) {
            return Some(parts);
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    eprintln!("honker: shm-fast-path disabled: failed to read stable -shm header.");
    None
}

/// Panics if the database has been replaced since startup. Returns
/// `true` on stat error so caller can fire a conservative wake. The
/// `-shm` file is intentionally not fatal: SQLite can truncate/recreate
/// it during normal WAL lifecycle churn, and the fast path can recover
/// by reopening the current file and rebasing `iChange`.
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
            eprintln!("honker: stat identity check failed for database file: {e}");
            true
        }
    }
}

/// Probe at `honker.open()` so a misconfigured backend errors
/// immediately instead of silently producing no wakes.
pub(crate) fn probe(db_path: &std::path::Path) -> Result<(), String> {
    if cfg!(target_endian = "big") {
        return Err("shm-fast-path requires little-endian platform".into());
    }
    let shm = format!("{}-shm", db_path.display());
    // Acquire through the registry, never `File::open` + drop. `probe`
    // runs at `honker.open()` time, *after* the caller's writer connection
    // has already created -wal/-shm, so a bare open+close here released
    // this process's SQLite WAL-index locks on every single open.
    let f = acquire_shm_fd(std::path::Path::new(&shm))
        .map_err(|e| format!("-shm unavailable ({e}). WAL mode + open connection required."))?;
    let header =
        read_wal_index_header(&f).map_err(|e| format!("-shm too small or unreadable: {e}"))?;
    let iv = u32::from_ne_bytes(header[0..4].try_into().unwrap());
    if iv != WALINDEX_MAX_VERSION {
        return Err(format!(
            "WAL index version {iv} != {WALINDEX_MAX_VERSION} (unsupported SQLite layout)"
        ));
    }
    Ok(())
}
