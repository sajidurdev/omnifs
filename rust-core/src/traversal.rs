use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crossbeam_channel::{Receiver, Sender, TrySendError};

use crate::filter::{FilterEngine, MatcherNode};
use crate::index::{normalize_path, DirFingerprintRecord, HashMode, IndexSnapshot};
use crate::stream::{BatchSender, FileRecord, SessionStats};

const DIR_FINGERPRINT_WEAK_THRESHOLD: u64 = 50_000;
const FAST_PATH_FIRST_BATCH_TARGET: usize = 16;
const FAST_PATH_INITIAL_BATCH_CAPACITY: usize = 64;
const FAST_PATH_BATCH_TARGET: usize = 4096;
const FAST_PATH_FIRST_FLUSH_INTERVAL_MS: u64 = 0;
const FAST_PATH_FLUSH_INTERVAL_MS: u64 = 4;
const SCALE_POLL_INTERVAL_MS: u64 = 1;

#[derive(Clone, Copy, Debug)]
pub struct TraversalConfig {
    pub threads: usize,
    pub deterministic: bool,
    pub hash_mode: HashMode,
    pub incremental: bool,
    pub force_hash: bool,
    pub fingerprinting: bool,
    pub fast_path: bool,
}

#[derive(Debug)]
pub struct PendingFile {
    pub path: PathBuf,
    pub record: FileRecord,
}

impl PendingFile {
    pub fn into_file_record(self, hash: Option<String>) -> FileRecord {
        FileRecord {
            path: self.record.path,
            size: self.record.size,
            mtime_ms: self.record.mtime_ms,
            identity: self.record.identity,
            hash,
        }
    }
}

#[derive(Debug)]
pub enum IndexInput {
    File(FileRecord),
    DirFingerprint(DirFingerprintRecord),
    Error(String),
}

#[derive(Debug)]
struct WorkItem {
    dir: PathBuf,
    dir_mtime_ms: u64,
    matcher: Arc<MatcherNode>,
}

pub fn run(
    root: PathBuf,
    filter: Arc<FilterEngine>,
    snapshot: Option<Arc<IndexSnapshot>>,
    meta_tx: Option<Sender<PendingFile>>,
    index_tx: Option<Sender<IndexInput>>,
    direct_sender: Option<BatchSender>,
    cancel: Arc<AtomicBool>,
    stats: Arc<SessionStats>,
    config: TraversalConfig,
) {
    let (queue_tx, queue_rx): (Sender<WorkItem>, Receiver<WorkItem>) =
        crossbeam_channel::unbounded();
    let pending_dirs = Arc::new(AtomicUsize::new(1));
    let root_matcher = filter.root_matcher();
    let root_mtime_ms = fs::metadata(&root)
        .map(|m| metadata_mtime_ms(&m))
        .unwrap_or(0);
    let _ = queue_tx.send(WorkItem {
        dir: root,
        dir_mtime_ms: root_mtime_ms,
        matcher: root_matcher,
    });

    let max_workers = config.threads.max(1);
    let mut workers = Vec::with_capacity(max_workers);
    let spawn_worker = |workers: &mut Vec<std::thread::JoinHandle<()>>| {
        let worker_queue_rx = queue_rx.clone();
        let worker_queue_tx = queue_tx.clone();
        let worker_pending = Arc::clone(&pending_dirs);
        let worker_filter = Arc::clone(&filter);
        let worker_snapshot = snapshot.as_ref().map(Arc::clone);
        let worker_meta_tx = meta_tx.as_ref().map(Sender::clone);
        let worker_index_tx = index_tx.as_ref().cloned();
        let worker_direct_sender = direct_sender.clone();
        let worker_cancel = Arc::clone(&cancel);
        let worker_stats = Arc::clone(&stats);
        let worker_config = config;

        workers.push(thread::spawn(move || {
            let mut matcher_cache = HashMap::<PathBuf, Arc<MatcherNode>>::new();
            let mut direct_batch = if worker_config.fast_path {
                Some(Vec::<FileRecord>::with_capacity(
                    FAST_PATH_INITIAL_BATCH_CAPACITY,
                ))
            } else {
                None
            };
            let mut first_batch_sent = false;
            let mut last_direct_flush = Instant::now();
            loop {
                if worker_cancel.load(Ordering::Relaxed) {
                    break;
                }
                match worker_queue_rx.recv_timeout(Duration::from_millis(50)) {
                    Ok(work) => {
                        process_directory(
                            work,
                            &worker_filter,
                            worker_snapshot.as_ref(),
                            &worker_meta_tx,
                            worker_index_tx.as_ref(),
                            worker_direct_sender.as_ref(),
                            &worker_queue_tx,
                            &worker_pending,
                            &worker_cancel,
                            &worker_stats,
                            &mut matcher_cache,
                            &mut direct_batch,
                            &mut first_batch_sent,
                            &mut last_direct_flush,
                            worker_config,
                        );
                        worker_pending.fetch_sub(1, Ordering::AcqRel);
                        flush_direct_batch_if_needed(
                            worker_direct_sender.as_ref(),
                            &mut direct_batch,
                            &mut first_batch_sent,
                            &mut last_direct_flush,
                            false,
                        );
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                        flush_direct_batch_if_needed(
                            worker_direct_sender.as_ref(),
                            &mut direct_batch,
                            &mut first_batch_sent,
                            &mut last_direct_flush,
                            false,
                        );
                        if worker_pending.load(Ordering::Acquire) == 0 {
                            break;
                        }
                    }
                    Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
                }
            }

            flush_direct_batch_if_needed(
                worker_direct_sender.as_ref(),
                &mut direct_batch,
                &mut first_batch_sent,
                &mut last_direct_flush,
                true,
            );
        }));
    };

    // Lazy worker
    // start with one worker and scale up only when queue/pending pressure indicates a larger tree
    spawn_worker(&mut workers);
    while workers.len() < max_workers {
        if cancel.load(Ordering::Relaxed) {
            break;
        }

        let pending = pending_dirs.load(Ordering::Acquire);
        if pending == 0 {
            break;
        }

        let queued = queue_rx.len();
        let desired_workers = pending.max(queued).max(1).min(max_workers);

        if desired_workers > workers.len() {
            let missing = desired_workers.saturating_sub(workers.len());
            for _ in 0..missing {
                spawn_worker(&mut workers);
            }
            continue;
        }

        thread::sleep(Duration::from_millis(SCALE_POLL_INTERVAL_MS));
    }

    drop(queue_tx);

    for worker in workers {
        let _ = worker.join();
    }
}

#[allow(clippy::too_many_arguments)]
fn process_directory(
    work: WorkItem,
    filter: &Arc<FilterEngine>,
    snapshot: Option<&Arc<IndexSnapshot>>,
    meta_tx: &Option<Sender<PendingFile>>,
    index_tx: Option<&Sender<IndexInput>>,
    direct_sender: Option<&BatchSender>,
    queue_tx: &Sender<WorkItem>,
    pending_dirs: &Arc<AtomicUsize>,
    cancel: &Arc<AtomicBool>,
    stats: &Arc<SessionStats>,
    matcher_cache: &mut HashMap<PathBuf, Arc<MatcherNode>>,
    direct_batch: &mut Option<Vec<FileRecord>>,
    first_batch_sent: &mut bool,
    last_direct_flush: &mut Instant,
    config: TraversalConfig,
) {
    if cancel.load(Ordering::Relaxed) {
        return;
    }

    stats.record_dirs_seen();
    let read_dir = match fs::read_dir(&work.dir) {
        Ok(entries) => entries,
        Err(err) => {
            stats.record_errors(1);
            let message = format!("read_dir failed for {}: {}", work.dir.display(), err);
            let _ = send_error(index_tx, direct_sender, message, cancel);
            return;
        }
    };

    let must_collect_entries =
        config.deterministic || (config.incremental && config.fingerprinting);
    if must_collect_entries {
        let mut entries = read_dir.filter_map(Result::ok).collect::<Vec<_>>();
        if config.deterministic {
            entries.sort_unstable_by_key(|entry| entry.file_name());
        }

        if config.incremental && config.fingerprinting {
            let relative_dir = relative_dir_key(filter, &work.dir);
            let fingerprint = build_dir_fingerprint(relative_dir, work.dir_mtime_ms, &entries);
            let should_skip = snapshot
                .map(|snap| snap.dir_fingerprint_matches(&fingerprint.dir_path, &fingerprint))
                .unwrap_or(false);
            if should_skip {
                stats.record_dirs_skipped_by_fingerprint();
                return;
            }
            let _ = send_index_input(index_tx, IndexInput::DirFingerprint(fingerprint), cancel);
        }

        for entry in entries {
            if !process_entry(
                entry,
                &work,
                filter,
                snapshot,
                meta_tx,
                index_tx,
                direct_sender,
                queue_tx,
                pending_dirs,
                cancel,
                stats,
                matcher_cache,
                direct_batch,
                first_batch_sent,
                last_direct_flush,
                config,
            ) {
                return;
            }
        }
        return;
    }

    for entry in read_dir {
        let Ok(entry) = entry else {
            continue;
        };
        if !process_entry(
            entry,
            &work,
            filter,
            snapshot,
            meta_tx,
            index_tx,
            direct_sender,
            queue_tx,
            pending_dirs,
            cancel,
            stats,
            matcher_cache,
            direct_batch,
            first_batch_sent,
            last_direct_flush,
            config,
        ) {
            return;
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn process_entry(
    entry: std::fs::DirEntry,
    work: &WorkItem,
    filter: &Arc<FilterEngine>,
    snapshot: Option<&Arc<IndexSnapshot>>,
    meta_tx: &Option<Sender<PendingFile>>,
    index_tx: Option<&Sender<IndexInput>>,
    direct_sender: Option<&BatchSender>,
    queue_tx: &Sender<WorkItem>,
    pending_dirs: &Arc<AtomicUsize>,
    cancel: &Arc<AtomicBool>,
    stats: &Arc<SessionStats>,
    matcher_cache: &mut HashMap<PathBuf, Arc<MatcherNode>>,
    direct_batch: &mut Option<Vec<FileRecord>>,
    first_batch_sent: &mut bool,
    last_direct_flush: &mut Instant,
    config: TraversalConfig,
) -> bool {
    if cancel.load(Ordering::Relaxed) {
        return false;
    }

    let path = entry.path();
    let file_type = match entry.file_type() {
        Ok(kind) => kind,
        Err(_) => return true,
    };
    let is_dir = file_type.is_dir();
    let matcher = if is_dir {
        filter.matcher_for_dir(Arc::clone(&work.matcher), &path, matcher_cache)
    } else {
        Arc::clone(&work.matcher)
    };

    if filter.is_excluded(&path, is_dir, &matcher) {
        return true;
    }

    if is_dir {
        let dir_mtime_ms = entry
            .metadata()
            .map(|meta| metadata_mtime_ms(&meta))
            .unwrap_or(0);
        pending_dirs.fetch_add(1, Ordering::AcqRel);
        if queue_tx
            .send(WorkItem {
                dir: path,
                dir_mtime_ms,
                matcher,
            })
            .is_err()
        {
            pending_dirs.fetch_sub(1, Ordering::AcqRel);
        }
        return true;
    }

    if !file_type.is_file() {
        return true;
    }
    let metadata = match entry.metadata() {
        Ok(meta) => meta,
        Err(_) => return true,
    };
    let file_record = FileRecord::from_path(&path, &metadata);
    stats.record_files_seen();
    if config.incremental {
        if let Some(snapshot) = snapshot {
            let relative_path = normalize_path(filter.root(), &path);
            let unchanged = snapshot.is_file_unchanged(
                &relative_path,
                file_record.size,
                file_record.mtime_ms,
                &file_record.identity,
            );
            let should_skip_unchanged = config.hash_mode == HashMode::Disabled
                || (config.hash_mode.is_enabled() && !config.force_hash);
            if unchanged && should_skip_unchanged {
                stats.record_files_skipped_by_index();
                return true;
            }
        }
    }

    let pending = PendingFile {
        path,
        record: file_record,
    };

    if config.hash_mode.is_enabled() {
        let Some(meta_tx) = meta_tx else {
            return true;
        };
        return send_pending_file(meta_tx, pending, stats, cancel);
    }

    if config.fast_path {
        if let (Some(sender), Some(local_batch)) = (direct_sender, direct_batch.as_mut()) {
            local_batch.push(pending.into_file_record(None));
            let flush_target = if *first_batch_sent {
                FAST_PATH_BATCH_TARGET
            } else {
                FAST_PATH_FIRST_BATCH_TARGET
            };
            if local_batch.len() >= flush_target {
                let mut ready = Vec::with_capacity(local_batch.capacity());
                std::mem::swap(local_batch, &mut ready);
                if sender.send_batch(ready).is_err() {
                    return false;
                }
                *first_batch_sent = true;
                *last_direct_flush = Instant::now();
            }
        }
        return true;
    }

    send_index_input(
        index_tx,
        IndexInput::File(pending.into_file_record(None)),
        cancel,
    )
}

fn send_pending_file(
    tx: &Sender<PendingFile>,
    pending: PendingFile,
    stats: &Arc<SessionStats>,
    cancel: &Arc<AtomicBool>,
) -> bool {
    if cancel.load(Ordering::Relaxed) {
        return false;
    }
    match tx.try_send(pending) {
        Ok(()) => true,
        Err(TrySendError::Full(pending)) => {
            stats.record_meta_queue_block_events(1);
            let blocked_at = Instant::now();
            let result = tx.send(pending).is_ok();
            let blocked_for = blocked_at.elapsed();
            if blocked_for >= Duration::from_micros(250) {
                stats.record_meta_queue_block_nanos(nanos_to_u64(blocked_for.as_nanos()));
            }
            result
        }
        Err(TrySendError::Disconnected(_)) => false,
    }
}

fn send_index_input(
    tx: Option<&Sender<IndexInput>>,
    input: IndexInput,
    cancel: &Arc<AtomicBool>,
) -> bool {
    if cancel.load(Ordering::Relaxed) {
        return false;
    }
    match tx {
        Some(sender) => sender.send(input).is_ok(),
        None => true,
    }
}

fn send_error(
    index_tx: Option<&Sender<IndexInput>>,
    direct_sender: Option<&BatchSender>,
    message: String,
    cancel: &Arc<AtomicBool>,
) -> bool {
    if let Some(sender) = direct_sender {
        return sender.send_error(message).is_ok();
    }
    send_index_input(index_tx, IndexInput::Error(message), cancel)
}

fn flush_direct_batch_if_needed(
    direct_sender: Option<&BatchSender>,
    direct_batch: &mut Option<Vec<FileRecord>>,
    first_batch_sent: &mut bool,
    last_direct_flush: &mut Instant,
    force: bool,
) {
    let Some(sender) = direct_sender else {
        return;
    };
    let Some(local_batch) = direct_batch.as_mut() else {
        return;
    };
    if local_batch.is_empty() {
        return;
    }
    let flush_target = if *first_batch_sent {
        FAST_PATH_BATCH_TARGET
    } else {
        FAST_PATH_FIRST_BATCH_TARGET
    };
    let flush_interval_ms = if *first_batch_sent {
        FAST_PATH_FLUSH_INTERVAL_MS
    } else {
        FAST_PATH_FIRST_FLUSH_INTERVAL_MS
    };
    if !force
        && local_batch.len() < flush_target
        && last_direct_flush.elapsed() < Duration::from_millis(flush_interval_ms)
    {
        return;
    }
    let mut ready = Vec::with_capacity(local_batch.capacity());
    std::mem::swap(local_batch, &mut ready);
    let _ = sender.send_batch(ready);
    *first_batch_sent = true;
    *last_direct_flush = Instant::now();
}

fn relative_dir_key(filter: &FilterEngine, path: &Path) -> String {
    let normalized = normalize_path(filter.root(), path);
    if normalized.is_empty() {
        ".".to_string()
    } else {
        normalized
    }
}

fn build_dir_fingerprint(
    relative_path: String,
    dir_mtime_ms: u64,
    entries: &[std::fs::DirEntry],
) -> DirFingerprintRecord {
    let child_count = entries.len() as u64;
    if child_count > DIR_FINGERPRINT_WEAK_THRESHOLD {
        return DirFingerprintRecord {
            dir_path: relative_path,
            dir_mtime_ms,
            child_count,
            name_hash: None,
            weak: true,
        };
    }

    let mut names = entries
        .iter()
        .map(|entry| entry.file_name().to_string_lossy().to_string())
        .collect::<Vec<_>>();
    names.sort_unstable();

    let mut hasher = blake3::Hasher::new();
    for name in names {
        hasher.update(name.as_bytes());
        hasher.update(&[0x1f]);
    }

    DirFingerprintRecord {
        dir_path: relative_path,
        dir_mtime_ms,
        child_count,
        name_hash: Some(hasher.finalize().to_hex().to_string()),
        weak: false,
    }
}

fn metadata_mtime_ms(metadata: &std::fs::Metadata) -> u64 {
    metadata
        .modified()
        .ok()
        .and_then(|mtime| mtime.duration_since(std::time::UNIX_EPOCH).ok())
        .map(|duration| duration.as_millis() as u64)
        .unwrap_or(0)
}

fn nanos_to_u64(nanos: u128) -> u64 {
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}
