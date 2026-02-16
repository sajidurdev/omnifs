use std::collections::VecDeque;
use std::fs::{self, Metadata};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant, UNIX_EPOCH};

use crossbeam_channel::{Receiver, Sender, TrySendError};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};

use crate::filter::FilterEngine;
use crate::index::{HashMode, IncrementalIndex, IndexSnapshot};
use crate::traversal::{self, IndexInput, PendingFile, TraversalConfig};

const OUTPUT_CHANNEL_CAPACITY: usize = 96;
const META_QUEUE_CAPACITY: usize = 16_384;
const INDEX_QUEUE_CAPACITY: usize = 8_192;
const DEFAULT_BATCH_SIZE: usize = 256;
const MIN_ADAPTIVE_BATCH: usize = 64;
const MAX_ADAPTIVE_BATCH: usize = 8192;
const INDEX_FLUSH_INTERVAL_MS: u64 = 6;
const HASH_FLUSH_INTERVAL_MS: u64 = 4;
const HASH_FIRST_FLUSH_INTERVAL_MS: u64 = 1;
const HASH_READ_BUF_SIZE: usize = 64 * 1024;
const HASH_FIRST_BATCH_TARGET: usize = 64;
const HASH_DIRECT_BATCH_TARGET: usize = 2048;
const FIRST_PULL_MIN_ITEMS: usize = 16;

struct PullState {
    dynamic_target: usize,
    last_pull_at: Instant,
    last_out_blocks: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FileRecord {
    pub path: String,
    pub size: u64,
    pub mtime_ms: u64,
    pub identity: String,
    pub hash: Option<String>,
}

impl FileRecord {
    pub fn from_path(path: &Path, meta: &Metadata) -> Self {
        let mtime_ms = meta
            .modified()
            .ok()
            .and_then(|modified| modified.duration_since(UNIX_EPOCH).ok())
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);

        Self {
            path: path.to_string_lossy().replace('\\', "/"),
            size: meta.len(),
            mtime_ms,
            identity: file_identity(meta),
            hash: None,
        }
    }
}

#[derive(Clone, Debug)]
pub enum StreamEvent {
    Batch(Vec<FileRecord>),
    Error(String),
    Done,
}

#[derive(Default)]
pub struct SessionStats {
    produced_items: AtomicU64,
    consumed_items: AtomicU64,
    max_inflight_items: AtomicU64,
    errors: AtomicU64,
    total_seen: AtomicU64,
    index_flush_ms: AtomicU64,
    finished: AtomicBool,
    dirs_seen: AtomicU64,
    files_seen: AtomicU64,
    files_emitted: AtomicU64,
    files_skipped_by_index: AtomicU64,
    dirs_skipped_by_fingerprint: AtomicU64,
    bytes_hashed: AtomicU64,
    batches_sent: AtomicU64,
    meta_queue_block_events: AtomicU64,
    out_queue_block_events: AtomicU64,
    meta_queue_block_nanos: AtomicU64,
    out_queue_block_nanos: AtomicU64,
    next_batch_calls: AtomicU64,
}

#[derive(Clone, Copy, Debug)]
pub struct SessionStatsSnapshot {
    pub produced_items: u64,
    pub consumed_items: u64,
    pub max_inflight_items: u64,
    pub errors: u64,
    pub total_seen: u64,
    pub index_flush_ms: u64,
    pub finished: bool,
    pub dirs_seen: u64,
    pub files_seen: u64,
    pub files_emitted: u64,
    pub files_skipped_by_index: u64,
    pub dirs_skipped_by_fingerprint: u64,
    pub bytes_hashed: u64,
    pub batches_sent: u64,
    pub meta_queue_block_events: u64,
    pub out_queue_block_events: u64,
    pub meta_queue_block_nanos: u64,
    pub out_queue_block_nanos: u64,
    pub next_batch_calls: u64,
}

impl SessionStats {
    pub fn record_batch_sent(&self, count: u64) {
        self.produced_items.fetch_add(count, Ordering::Relaxed);
        self.files_emitted.fetch_add(count, Ordering::Relaxed);
        self.batches_sent.fetch_add(1, Ordering::Relaxed);
        self.update_max_inflight();
    }

    pub fn record_consumed(&self, count: u64) {
        self.consumed_items.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_errors(&self, count: u64) {
        self.errors.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_dirs_seen(&self) {
        self.dirs_seen.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_files_seen(&self) {
        let total = self.total_seen.fetch_add(1, Ordering::Relaxed) + 1;
        self.files_seen.store(total, Ordering::Relaxed);
    }

    pub fn record_files_skipped_by_index(&self) {
        self.files_skipped_by_index.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_dirs_skipped_by_fingerprint(&self) {
        self.dirs_skipped_by_fingerprint
            .fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_bytes_hashed(&self, bytes: u64) {
        self.bytes_hashed.fetch_add(bytes, Ordering::Relaxed);
    }

    pub fn record_meta_queue_block_events(&self, count: u64) {
        self.meta_queue_block_events
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_meta_queue_block_nanos(&self, nanos: u64) {
        self.meta_queue_block_nanos
            .fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn record_out_queue_block_events(&self, count: u64) {
        self.out_queue_block_events
            .fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_out_queue_block_nanos(&self, nanos: u64) {
        self.out_queue_block_nanos
            .fetch_add(nanos, Ordering::Relaxed);
    }

    pub fn record_next_batch_call(&self) {
        self.next_batch_calls.fetch_add(1, Ordering::Relaxed);
    }

    pub fn set_index_flush_ms(&self, value: u64) {
        self.index_flush_ms.store(value, Ordering::Relaxed);
    }

    pub fn mark_finished(&self) {
        self.finished.store(true, Ordering::Release);
    }

    pub fn snapshot(&self) -> SessionStatsSnapshot {
        SessionStatsSnapshot {
            produced_items: self.produced_items.load(Ordering::Relaxed),
            consumed_items: self.consumed_items.load(Ordering::Relaxed),
            max_inflight_items: self.max_inflight_items.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            total_seen: self.total_seen.load(Ordering::Relaxed),
            index_flush_ms: self.index_flush_ms.load(Ordering::Relaxed),
            finished: self.finished.load(Ordering::Acquire),
            dirs_seen: self.dirs_seen.load(Ordering::Relaxed),
            files_seen: self.files_seen.load(Ordering::Relaxed),
            files_emitted: self.files_emitted.load(Ordering::Relaxed),
            files_skipped_by_index: self.files_skipped_by_index.load(Ordering::Relaxed),
            dirs_skipped_by_fingerprint: self.dirs_skipped_by_fingerprint.load(Ordering::Relaxed),
            bytes_hashed: self.bytes_hashed.load(Ordering::Relaxed),
            batches_sent: self.batches_sent.load(Ordering::Relaxed),
            meta_queue_block_events: self.meta_queue_block_events.load(Ordering::Relaxed),
            out_queue_block_events: self.out_queue_block_events.load(Ordering::Relaxed),
            meta_queue_block_nanos: self.meta_queue_block_nanos.load(Ordering::Relaxed),
            out_queue_block_nanos: self.out_queue_block_nanos.load(Ordering::Relaxed),
            next_batch_calls: self.next_batch_calls.load(Ordering::Relaxed),
        }
    }

    fn update_max_inflight(&self) {
        let produced = self.produced_items.load(Ordering::Relaxed);
        let consumed = self.consumed_items.load(Ordering::Relaxed);
        let inflight = produced.saturating_sub(consumed);
        let mut current = self.max_inflight_items.load(Ordering::Relaxed);
        while inflight > current {
            match self.max_inflight_items.compare_exchange_weak(
                current,
                inflight,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => current = observed,
            }
        }
    }
}

#[derive(Clone)]
pub struct BatchSender {
    tx: Sender<StreamEvent>,
    stats: Arc<SessionStats>,
}

impl BatchSender {
    pub fn new(tx: Sender<StreamEvent>, stats: Arc<SessionStats>) -> Self {
        Self { tx, stats }
    }

    pub fn send_batch(&self, batch: Vec<FileRecord>) -> Result<(), String> {
        let count = batch.len() as u64;
        match self.tx.try_send(StreamEvent::Batch(batch)) {
            Ok(()) => {
                self.stats.record_batch_sent(count);
                Ok(())
            }
            Err(TrySendError::Full(StreamEvent::Batch(batch))) => {
                self.stats.record_out_queue_block_events(1);
                let blocked_at = Instant::now();
                self.tx
                    .send(StreamEvent::Batch(batch))
                    .map_err(|e| e.to_string())?;
                self.stats
                    .record_out_queue_block_nanos(nanos_to_u64(blocked_at.elapsed().as_nanos()));
                self.stats.record_batch_sent(count);
                Ok(())
            }
            Err(TrySendError::Disconnected(_)) => Err("stream output disconnected".to_string()),
            Err(TrySendError::Full(_)) => Err("unexpected stream event".to_string()),
        }
    }

    pub fn send_error(&self, message: String) -> Result<(), String> {
        self.stats.record_errors(1);
        self.tx
            .send(StreamEvent::Error(message))
            .map_err(|e| e.to_string())
    }

    pub fn finish(&self) -> Result<(), String> {
        self.tx.send(StreamEvent::Done).map_err(|e| e.to_string())
    }
}

pub struct DiscoverySession {
    pub cancel: Arc<AtomicBool>,
    done: AtomicBool,
    rx: Receiver<StreamEvent>,
    stash: Mutex<VecDeque<FileRecord>>,
    stats: Arc<SessionStats>,
    pull: Mutex<PullState>,
}

impl DiscoverySession {
    pub fn next_batch_blocking(&self, max_items: usize) -> Result<Option<Vec<FileRecord>>, String> {
        self.stats.record_next_batch_call();
        let is_first_pull = self.stats.snapshot().next_batch_calls <= 1;
        if self.done.load(Ordering::Acquire) {
            let stash = self.stash.lock();
            if stash.is_empty() {
                return Ok(None);
            }
        }

        let target = self.next_pull_target(max_items.max(1));
        let mut output = Vec::with_capacity(target);
        self.drain_stash(target, &mut output);
        if output.len() >= target {
            self.stats.record_consumed(output.len() as u64);
            return Ok(Some(output));
        }

        loop {
            match self.rx.recv_timeout(Duration::from_millis(100)) {
                Ok(StreamEvent::Batch(batch)) => {
                    {
                        let mut stash = self.stash.lock();
                        stash.extend(batch);
                    }
                    self.drain_stash(target, &mut output);
                    if output.len() >= target {
                        break;
                    }
                    if is_first_pull && output.len() >= FIRST_PULL_MIN_ITEMS {
                        break;
                    }
                }
                Ok(StreamEvent::Error(message)) => return Err(message),
                Ok(StreamEvent::Done) => {
                    self.done.store(true, Ordering::Release);
                    self.drain_stash(target, &mut output);
                    break;
                }
                Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                    if self.done.load(Ordering::Acquire) || self.cancel.load(Ordering::Relaxed) {
                        self.done.store(true, Ordering::Release);
                        self.drain_stash(target, &mut output);
                        break;
                    }
                }
                Err(crossbeam_channel::RecvTimeoutError::Disconnected) => {
                    self.done.store(true, Ordering::Release);
                    self.drain_stash(target, &mut output);
                    break;
                }
            }
        }

        if output.is_empty() && self.done.load(Ordering::Acquire) {
            Ok(None)
        } else {
            self.stats.record_consumed(output.len() as u64);
            Ok(Some(output))
        }
    }

    fn drain_stash(&self, max_items: usize, output: &mut Vec<FileRecord>) {
        if output.len() >= max_items {
            return;
        }

        let needed = max_items.saturating_sub(output.len());
        let mut stash = self.stash.lock();
        for _ in 0..needed {
            if let Some(item) = stash.pop_front() {
                output.push(item);
            } else {
                break;
            }
        }
    }

    pub fn metrics_snapshot(&self) -> SessionStatsSnapshot {
        self.stats.snapshot()
    }

    fn next_pull_target(&self, requested: usize) -> usize {
        let snapshot = self.stats.snapshot();
        let inflight = snapshot
            .produced_items
            .saturating_sub(snapshot.consumed_items);
        let now = Instant::now();
        let mut pull = self.pull.lock();
        let elapsed = now.duration_since(pull.last_pull_at);

        if snapshot.out_queue_block_events > pull.last_out_blocks || inflight > 4_096 {
            pull.dynamic_target = (pull.dynamic_target * 2).min(MAX_ADAPTIVE_BATCH);
        } else if elapsed > Duration::from_millis(10) && inflight > 0 {
            // Slow consumer cadence means larger pulls reduce JS<->native crossings.
            pull.dynamic_target = (pull.dynamic_target * 2).min(MAX_ADAPTIVE_BATCH);
        } else if elapsed < Duration::from_millis(5) && inflight == 0 {
            pull.dynamic_target = (pull.dynamic_target / 2).max(MIN_ADAPTIVE_BATCH);
        }

        pull.last_pull_at = now;
        pull.last_out_blocks = snapshot.out_queue_block_events;
        requested.max(pull.dynamic_target).min(MAX_ADAPTIVE_BATCH)
    }
}

pub struct SessionIndexState {
    pub snapshot: Option<Arc<IndexSnapshot>>,
    pub writer: Option<IncrementalIndex>,
}

static SESSION_THREAD_COUNTER: AtomicU64 = AtomicU64::new(1);

pub fn spawn_session(
    root: PathBuf,
    filter: Arc<FilterEngine>,
    index_state: SessionIndexState,
    traversal_config: TraversalConfig,
    initial_batch_size: usize,
) -> Arc<DiscoverySession> {
    let stats = Arc::new(SessionStats::default());
    let (tx, rx) = crossbeam_channel::bounded::<StreamEvent>(OUTPUT_CHANNEL_CAPACITY);
    let cancel = Arc::new(AtomicBool::new(false));
    let session = Arc::new(DiscoverySession {
        cancel: Arc::clone(&cancel),
        done: AtomicBool::new(false),
        rx,
        stash: Mutex::new(VecDeque::new()),
        stats: Arc::clone(&stats),
        pull: Mutex::new(PullState {
            dynamic_target: initial_batch_size
                .max(MIN_ADAPTIVE_BATCH)
                .min(MAX_ADAPTIVE_BATCH),
            last_pull_at: Instant::now(),
            last_out_blocks: 0,
        }),
    });

    let thread_name = format!(
        "omnifs-session-{}",
        SESSION_THREAD_COUNTER.fetch_add(1, Ordering::Relaxed)
    );
    let sender = BatchSender::new(tx, Arc::clone(&stats));
    let spawn_stats = Arc::clone(&stats);
    let spawn_sender = sender.clone();
    let spawn_result = std::thread::Builder::new()
        .name(thread_name)
        .spawn(move || {
            run_pipeline(
                root,
                filter,
                index_state,
                traversal_config,
                initial_batch_size,
                sender,
                cancel,
                spawn_stats,
            );
        });
    if let Err(err) = spawn_result {
        stats.record_errors(1);
        let _ = spawn_sender.send_error(format!("failed to spawn discovery thread: {}", err));
        stats.mark_finished();
        let _ = spawn_sender.finish();
    }

    session
}

fn run_pipeline(
    root: PathBuf,
    filter: Arc<FilterEngine>,
    index_state: SessionIndexState,
    traversal_config: TraversalConfig,
    initial_batch_size: usize,
    sender: BatchSender,
    cancel: Arc<AtomicBool>,
    stats: Arc<SessionStats>,
) {
    let SessionIndexState {
        snapshot: index_snapshot,
        writer: index_writer,
    } = index_state;
    let direct_stream_mode =
        !traversal_config.hash_mode.is_enabled() && !traversal_config.incremental;
    let hash_direct_mode = traversal_config.hash_mode.is_enabled() && !traversal_config.incremental;
    if direct_stream_mode {
        // Non-incremental + no-hash scans should stream directly from traversal workers
        let mut direct_config = traversal_config;
        direct_config.fast_path = true;
        traversal::run(
            root,
            filter,
            index_snapshot,
            None,
            None,
            Some(sender.clone()),
            Arc::clone(&cancel),
            Arc::clone(&stats),
            direct_config,
        );
        stats.mark_finished();
        let _ = sender.finish();
        return;
    }

    if hash_direct_mode {
        // Non-incremental hash scans can bypass the indexing stage and stream directly from hashers
        let (meta_tx, meta_rx) = crossbeam_channel::bounded::<PendingFile>(META_QUEUE_CAPACITY);
        let hasher_count =
            hasher_worker_count(traversal_config.threads, traversal_config.hash_mode);
        let mut handles = Vec::with_capacity(hasher_count);
        for _ in 0..hasher_count {
            let worker_rx = meta_rx.clone();
            let worker_sender = sender.clone();
            let worker_cancel = Arc::clone(&cancel);
            let worker_stats = Arc::clone(&stats);
            handles.push(std::thread::spawn(move || {
                run_hasher_direct(
                    worker_rx,
                    worker_sender,
                    worker_cancel,
                    worker_stats,
                    initial_batch_size.max(HASH_DIRECT_BATCH_TARGET),
                    traversal_config.hash_mode,
                );
            }));
        }

        traversal::run(
            root,
            filter,
            index_snapshot,
            Some(meta_tx.clone()),
            None,
            None,
            Arc::clone(&cancel),
            Arc::clone(&stats),
            traversal_config,
        );

        drop(meta_tx);
        for handle in handles {
            let _ = handle.join();
        }

        stats.mark_finished();
        let _ = sender.finish();
        return;
    }
    let (index_tx, index_rx) = crossbeam_channel::bounded::<IndexInput>(INDEX_QUEUE_CAPACITY);

    let meta_pipeline = if traversal_config.hash_mode.is_enabled() {
        let (meta_tx, meta_rx) = crossbeam_channel::bounded::<PendingFile>(META_QUEUE_CAPACITY);
        let hasher_count =
            hasher_worker_count(traversal_config.threads, traversal_config.hash_mode);
        let mut handles = Vec::with_capacity(hasher_count);
        for _ in 0..hasher_count {
            let worker_rx = meta_rx.clone();
            let worker_index_tx = index_tx.clone();
            let worker_cancel = Arc::clone(&cancel);
            let worker_stats = Arc::clone(&stats);
            let worker_hash_mode = traversal_config.hash_mode;
            handles.push(std::thread::spawn(move || {
                run_hasher(
                    worker_rx,
                    worker_index_tx,
                    worker_cancel,
                    worker_stats,
                    worker_hash_mode,
                );
            }));
        }
        Some((meta_tx, handles))
    } else {
        None
    };

    let indexer_cancel = Arc::clone(&cancel);
    let indexer_stats = Arc::clone(&stats);
    let indexer_sender = sender.clone();
    let indexer_initial_batch = initial_batch_size;
    let indexer_handle = std::thread::spawn(move || {
        run_indexer(
            index_rx,
            index_writer,
            indexer_sender,
            indexer_cancel,
            indexer_stats,
            indexer_initial_batch,
        );
    });

    traversal::run(
        root,
        filter,
        index_snapshot,
        meta_pipeline.as_ref().map(|(tx, _)| tx.clone()),
        Some(index_tx.clone()),
        None,
        Arc::clone(&cancel),
        Arc::clone(&stats),
        traversal_config,
    );

    if let Some((meta_tx, handles)) = meta_pipeline {
        drop(meta_tx);
        for handle in handles {
            let _ = handle.join();
        }
    }

    drop(index_tx);
    let _ = indexer_handle.join();
}

fn run_hasher(
    meta_rx: Receiver<PendingFile>,
    index_tx: Sender<IndexInput>,
    cancel: Arc<AtomicBool>,
    stats: Arc<SessionStats>,
    hash_mode: HashMode,
) {
    let mut read_buf = vec![0_u8; HASH_READ_BUF_SIZE];
    loop {
        match meta_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(pending) => {
                let hash = match compute_hash(&pending, hash_mode, &mut read_buf) {
                    Ok(value) => {
                        stats.record_bytes_hashed(pending.record.size);
                        Some(value)
                    }
                    Err(err) => {
                        let _ = index_tx.send(IndexInput::Error(format!(
                            "hash failed for {}: {}",
                            pending.path.display(),
                            err
                        )));
                        None
                    }
                };
                if index_tx
                    .send(IndexInput::File(pending.into_file_record(hash)))
                    .is_err()
                {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if cancel.load(Ordering::Relaxed) && meta_rx.is_empty() {
                    break;
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }
}

fn run_hasher_direct(
    meta_rx: Receiver<PendingFile>,
    sender: BatchSender,
    cancel: Arc<AtomicBool>,
    stats: Arc<SessionStats>,
    initial_batch_size: usize,
    hash_mode: HashMode,
) {
    let mut batch_target = initial_batch_size
        .max(HASH_DIRECT_BATCH_TARGET)
        .min(MAX_ADAPTIVE_BATCH);
    let mut batch = Vec::<FileRecord>::with_capacity(batch_target);
    let mut first_flush_sent = false;
    let mut last_flush_at = Instant::now();
    let mut last_out_blocks = 0_u64;
    let mut last_consumed = 0_u64;
    let mut read_buf = vec![0_u8; HASH_READ_BUF_SIZE];

    loop {
        match meta_rx.recv_timeout(Duration::from_millis(20)) {
            Ok(pending) => match compute_hash(&pending, hash_mode, &mut read_buf) {
                Ok(value) => {
                    stats.record_bytes_hashed(pending.record.size);
                    batch.push(pending.into_file_record(Some(value)));
                    let flush_target = if first_flush_sent {
                        batch_target
                    } else {
                        HASH_FIRST_BATCH_TARGET
                    };
                    if batch.len() >= flush_target {
                        if flush_batch(&sender, &mut batch).is_err() {
                            break;
                        }
                        first_flush_sent = true;
                        last_flush_at = Instant::now();
                        adapt_batch_size(
                            &stats,
                            &mut batch_target,
                            &mut last_out_blocks,
                            &mut last_consumed,
                        );
                    }
                }
                Err(err) => {
                    let _ = sender.send_error(format!(
                        "hash failed for {}: {}",
                        pending.path.display(),
                        err
                    ));
                }
            },
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if cancel.load(Ordering::Relaxed) && meta_rx.is_empty() {
                    break;
                }
                let flush_interval_ms = if first_flush_sent {
                    HASH_FLUSH_INTERVAL_MS
                } else {
                    HASH_FIRST_FLUSH_INTERVAL_MS
                };
                if !batch.is_empty()
                    && last_flush_at.elapsed() >= Duration::from_millis(flush_interval_ms)
                {
                    if flush_batch(&sender, &mut batch).is_err() {
                        break;
                    }
                    first_flush_sent = true;
                    last_flush_at = Instant::now();
                    adapt_batch_size(
                        &stats,
                        &mut batch_target,
                        &mut last_out_blocks,
                        &mut last_consumed,
                    );
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }

    let _ = flush_batch(&sender, &mut batch);
}

fn run_indexer(
    index_rx: Receiver<IndexInput>,
    mut index_writer: Option<IncrementalIndex>,
    sender: BatchSender,
    cancel: Arc<AtomicBool>,
    stats: Arc<SessionStats>,
    initial_batch_size: usize,
) {
    let mut batch_target = initial_batch_size
        .max(MIN_ADAPTIVE_BATCH)
        .min(MAX_ADAPTIVE_BATCH);
    if batch_target == 0 {
        batch_target = DEFAULT_BATCH_SIZE;
    }
    let mut last_out_blocks = 0_u64;
    let mut last_consumed = 0_u64;
    let mut batch = Vec::<FileRecord>::with_capacity(batch_target);
    let mut last_flush_at = Instant::now();

    loop {
        match index_rx.recv_timeout(Duration::from_millis(50)) {
            Ok(IndexInput::File(file)) => {
                let should_emit = if let Some(index) = index_writer.as_mut() {
                    index.should_emit_and_update(&file)
                } else {
                    true
                };
                if should_emit {
                    batch.push(file);
                    if batch.len() >= batch_target {
                        if flush_batch(&sender, &mut batch).is_err() {
                            break;
                        }
                        last_flush_at = Instant::now();
                        adapt_batch_size(
                            &stats,
                            &mut batch_target,
                            &mut last_out_blocks,
                            &mut last_consumed,
                        );
                    }
                } else {
                    stats.record_files_skipped_by_index();
                }
            }
            Ok(IndexInput::DirFingerprint(record)) => {
                if let Some(index) = index_writer.as_mut() {
                    index.update_dir_fingerprint(record);
                }
            }
            Ok(IndexInput::Error(message)) => {
                let _ = sender.send_error(message);
            }
            Err(crossbeam_channel::RecvTimeoutError::Timeout) => {
                if cancel.load(Ordering::Relaxed) && index_rx.is_empty() {
                    break;
                }
                if !batch.is_empty()
                    && last_flush_at.elapsed() >= Duration::from_millis(INDEX_FLUSH_INTERVAL_MS)
                {
                    if flush_batch(&sender, &mut batch).is_err() {
                        break;
                    }
                    last_flush_at = Instant::now();
                    adapt_batch_size(
                        &stats,
                        &mut batch_target,
                        &mut last_out_blocks,
                        &mut last_consumed,
                    );
                }
            }
            Err(crossbeam_channel::RecvTimeoutError::Disconnected) => break,
        }
    }

    let _ = flush_batch(&sender, &mut batch);
    if let Some(index) = index_writer.as_mut() {
        let flush_start = std::time::Instant::now();
        if let Err(err) = index.flush() {
            let _ = sender.send_error(format!("failed to flush incremental index: {}", err));
        }
        stats.set_index_flush_ms(flush_start.elapsed().as_millis() as u64);
    }
    stats.mark_finished();
    let _ = sender.finish();
}

fn flush_batch(sender: &BatchSender, batch: &mut Vec<FileRecord>) -> Result<(), String> {
    if batch.is_empty() {
        return Ok(());
    }
    let mut ready = Vec::with_capacity(batch.capacity());
    std::mem::swap(batch, &mut ready);
    sender.send_batch(ready)
}

fn adapt_batch_size(
    stats: &Arc<SessionStats>,
    batch_target: &mut usize,
    last_out_blocks: &mut u64,
    last_consumed: &mut u64,
) {
    let snapshot = stats.snapshot();
    let inflight = snapshot
        .produced_items
        .saturating_sub(snapshot.consumed_items);
    if snapshot.out_queue_block_events > *last_out_blocks || inflight > (*batch_target as u64 * 4) {
        *batch_target = (*batch_target * 2).min(MAX_ADAPTIVE_BATCH);
    } else if inflight == 0 && snapshot.consumed_items > *last_consumed {
        *batch_target = (*batch_target / 2).max(MIN_ADAPTIVE_BATCH);
    }
    *last_out_blocks = snapshot.out_queue_block_events;
    *last_consumed = snapshot.consumed_items;
}

fn nanos_to_u64(nanos: u128) -> u64 {
    if nanos > u64::MAX as u128 {
        u64::MAX
    } else {
        nanos as u64
    }
}

fn compute_blake3(path: &Path, buf: &mut [u8]) -> std::io::Result<String> {
    let mut file = fs::File::open(path)?;
    let mut hasher = blake3::Hasher::new();
    loop {
        let read = file.read(buf)?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn compute_hash(
    pending: &PendingFile,
    hash_mode: HashMode,
    read_buf: &mut [u8],
) -> std::io::Result<String> {
    match hash_mode {
        HashMode::Blake3 => compute_blake3(&pending.path, read_buf),
        HashMode::Disabled => Ok(String::new()),
    }
}

fn hasher_worker_count(threads: usize, hash_mode: HashMode) -> usize {
    if hash_mode.is_enabled() {
        threads.max(1).min(8)
    } else {
        1
    }
}

#[cfg(unix)]
fn file_identity(meta: &Metadata) -> String {
    use std::os::unix::fs::MetadataExt;
    format!("{}:{}", meta.dev(), meta.ino())
}

#[cfg(windows)]
fn file_identity(meta: &Metadata) -> String {
    use std::os::windows::fs::MetadataExt;
    format!("{}:{}", meta.last_write_time(), meta.file_size())
}

#[cfg(not(any(unix, windows)))]
fn file_identity(meta: &Metadata) -> String {
    format!("{}:{}", meta.len(), 0_u64)
}
