//! Optional `-shm` fast path (feature = `shm-fast-path`).
//!
//! **Experimental.** Weaker correctness contract than the polling
//! backend, in exchange for sub-millisecond wake latency.
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
//! Tests assert that wakes fire with sub-millisecond latency in WAL
//! mode. If a test fails, the backend is broken — not "fall back to
//! polling and pretend it worked".

use crate::stat_identity;
use rusqlite::{Connection, OpenFlags};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

const WALINDEX_MAX_VERSION: u32 = 3_007_000;
const ICHANGE_OFFSET: usize = 8;
/// Same cadence as the polling backend. Shm reads are nearly free; the
/// win over polling is "PRAGMA → memory load" (~3.5 µs → ns), not
/// "1 ms → 100 µs". Going faster would just burn extra sleep syscalls
/// for latency nobody can perceive.
const POLL_INTERVAL_MS: u64 = 1;
/// Cadence for the dead-man's switch (db / -shm replacement detection).
/// Same wall-clock interval as the polling and kernel backends. Tracked
/// via Instant — tick counting drifts on Windows where 1 ms sleeps round
/// up to ~15 ms.
const IDENTITY_CHECK_INTERVAL: Duration = Duration::from_millis(100);

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
        let current = match read_wal_index_header(&mut f) {
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

fn read_wal_index_header(file: &mut File) -> std::io::Result<[u8; 12]> {
    let mut header = [0_u8; 12];
    file.seek(SeekFrom::Start(0))?;
    file.read_exact(&mut header)?;
    Ok(header)
}

fn read_ichange_from_header(header: &[u8; 12]) -> u32 {
    u32::from_ne_bytes(
        header[ICHANGE_OFFSET..ICHANGE_OFFSET + 4]
            .try_into()
            .unwrap(),
    )
}

fn reopen_shm_header(path: &std::path::Path) -> Option<(File, [u8; 12], (u64, u64))> {
    let mut file = File::open(path).ok()?;
    let header = read_wal_index_header(&mut file).ok()?;
    let id = stat_identity(path).ok()?;
    Some((file, header, id))
}

fn wait_for_initial_shm_header(
    path: &std::path::Path,
    stop: &AtomicBool,
) -> Option<(File, [u8; 12], (u64, u64))> {
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
    let mut f = File::open(&shm)
        .map_err(|e| format!("-shm unavailable ({e}). WAL mode + open connection required."))?;
    let header =
        read_wal_index_header(&mut f).map_err(|e| format!("-shm too small or unreadable: {e}"))?;
    let iv = u32::from_ne_bytes(header[0..4].try_into().unwrap());
    if iv != WALINDEX_MAX_VERSION {
        return Err(format!(
            "WAL index version {iv} != {WALINDEX_MAX_VERSION} (unsupported SQLite layout)"
        ));
    }
    Ok(())
}
