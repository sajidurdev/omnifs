use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use napi_derive::napi;
use once_cell::sync::Lazy;
use parking_lot::Mutex;

mod filter;
mod index;
mod stream;
mod traversal;

use filter::{FilterEngine, FilterOptions};
use index::{HashMode, IncrementalIndex};
use stream::{DiscoverySession, SessionIndexState};
use traversal::TraversalConfig;

static NEXT_SESSION_ID: AtomicU32 = AtomicU32::new(1);
static SESSIONS: Lazy<Mutex<HashMap<u32, Arc<DiscoverySession>>>> =
    Lazy::new(|| Mutex::new(HashMap::new()));

#[napi(object)]
pub struct NativeDiscoverOptions {
    pub patterns: Option<Vec<String>>,
    pub respect_gitignore: Option<bool>,
    pub incremental: Option<bool>,
    pub hash: Option<String>,
    pub mode: Option<String>,
    pub fast_path: Option<String>,
    pub fingerprinting: Option<bool>,
    pub force_hash: Option<bool>,
    pub deterministic: Option<bool>,
    pub batch_size: Option<u32>,
    pub threads: Option<u32>,
}

#[napi(object)]
pub struct FileEntry {
    pub path: String,
    pub size: i64,
    pub mtime_ms: i64,
    pub identity: String,
    pub hash: Option<String>,
}

#[napi(object)]
pub struct SessionMetrics {
    pub produced_items: i64,
    pub consumed_items: i64,
    pub max_inflight_items: i64,
    pub errors: i64,
    pub total_seen: i64,
    pub index_flush_ms: i64,
    pub dirs_seen: i64,
    pub files_seen: i64,
    pub files_emitted: i64,
    pub files_skipped_by_index: i64,
    pub dirs_skipped_by_fingerprint: i64,
    pub bytes_hashed: i64,
    pub batches_sent: i64,
    pub meta_queue_block_events: i64,
    pub out_queue_block_events: i64,
    pub meta_queue_block_nanos: i64,
    pub out_queue_block_nanos: i64,
    pub next_batch_calls: i64,
    pub finished: bool,
}

impl From<stream::FileRecord> for FileEntry {
    fn from(value: stream::FileRecord) -> Self {
        Self {
            path: value.path,
            size: value.size.try_into().unwrap_or(i64::MAX),
            mtime_ms: value.mtime_ms.try_into().unwrap_or(i64::MAX),
            identity: value.identity,
            hash: value.hash,
        }
    }
}

#[napi]
pub fn start_discovery(root: String, options: Option<NativeDiscoverOptions>) -> napi::Result<u32> {
    let root_path = PathBuf::from(root);
    let options = options.unwrap_or(NativeDiscoverOptions {
        patterns: None,
        respect_gitignore: None,
        incremental: None,
        hash: None,
        mode: None,
        fast_path: None,
        fingerprinting: None,
        force_hash: None,
        deterministic: None,
        batch_size: None,
        threads: None,
    });

    let mode = options.mode.clone().unwrap_or_else(|| "auto".to_string());
    let respect_gitignore = options.respect_gitignore.unwrap_or(true) && mode != "glob";
    let patterns = if mode == "ignore" {
        Vec::new()
    } else {
        options.patterns.unwrap_or_default()
    };
    let patterns_present = !patterns.is_empty();

    let filter = FilterEngine::new(
        root_path.clone(),
        FilterOptions {
            patterns,
            respect_gitignore,
        },
    )
    .map_err(|e| napi::Error::from_reason(e.to_string()))?;
    let filter = Arc::new(filter);

    let hash_mode = HashMode::from_option(options.hash.as_deref());
    let incremental_requested = options.incremental.unwrap_or(false) && mode != "glob";
    let fast_path_pref = options
        .fast_path
        .clone()
        .unwrap_or_else(|| "auto".to_string());
    let fast_path_compatible = !hash_mode.is_enabled() && !incremental_requested;
    let auto_fast_path =
        (mode == "glob" || (patterns_present && !respect_gitignore)) && fast_path_compatible;
    // Fast path bypasses incremental index writing / loading and streams traversal results directly
    let fast_path_enabled = match fast_path_pref.as_str() {
        "glob" => fast_path_compatible,
        "none" => false,
        _ => auto_fast_path,
    };

    let incremental = incremental_requested && !fast_path_enabled;
    let index_state = if incremental {
        let writer = IncrementalIndex::open(root_path.clone())
            .map_err(|e| napi::Error::from_reason(e.to_string()))?;
        let snapshot = Arc::new(writer.snapshot());
        SessionIndexState {
            snapshot: Some(snapshot),
            writer: Some(writer),
        }
    } else {
        SessionIndexState {
            snapshot: None,
            writer: None,
        }
    };

    let deterministic = options.deterministic.unwrap_or(false);
    let thread_count = if deterministic {
        // Deterministic mode is stable but sadly slower single-threaded traversal + per-dir sorting
        1
    } else {
        options
            .threads
            .map(|value| value.max(1) as usize)
            .unwrap_or_else(|| num_cpus::get_physical().max(1))
    };
    let fingerprinting = if incremental {
        options.fingerprinting.unwrap_or(true)
    } else {
        false
    };
    let force_hash = options.force_hash.unwrap_or(false);
    let initial_batch_size = options.batch_size.unwrap_or(256).max(1) as usize;

    let traversal_config = TraversalConfig {
        threads: thread_count,
        deterministic,
        hash_mode,
        incremental,
        force_hash,
        fingerprinting,
        fast_path: fast_path_enabled,
    };

    let id = NEXT_SESSION_ID.fetch_add(1, Ordering::Relaxed);
    let session = stream::spawn_session(
        root_path,
        filter,
        index_state,
        traversal_config,
        initial_batch_size,
    );
    SESSIONS.lock().insert(id, session);
    Ok(id)
}

#[napi]
pub async fn next_batch(
    session_id: u32,
    max_items: Option<u32>,
) -> napi::Result<Option<Vec<FileEntry>>> {
    let session = {
        let sessions = SESSIONS.lock();
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| napi::Error::from_reason(format!("unknown session id {}", session_id)))?
    };

    let limit = max_items.unwrap_or(256).max(1) as usize;
    let batch = tokio::task::spawn_blocking(move || session.next_batch_blocking(limit))
        .await
        .map_err(|err| napi::Error::from_reason(format!("session task failed: {}", err)))?
        .map_err(napi::Error::from_reason)?;

    Ok(batch.map(|records| records.into_iter().map(FileEntry::from).collect()))
}

#[napi]
pub fn cancel_discovery(session_id: u32) -> napi::Result<()> {
    let maybe_session = SESSIONS.lock().get(&session_id).cloned();
    if let Some(session) = maybe_session {
        session.cancel.store(true, Ordering::Relaxed);
        Ok(())
    } else {
        Err(napi::Error::from_reason(format!(
            "unknown session id {}",
            session_id
        )))
    }
}

#[napi]
pub fn close_discovery(session_id: u32) -> napi::Result<()> {
    let session = SESSIONS.lock().remove(&session_id);
    if let Some(session) = session {
        session.cancel.store(true, Ordering::Relaxed);
    }
    Ok(())
}

#[napi]
pub fn session_metrics(session_id: u32) -> napi::Result<SessionMetrics> {
    let session = {
        let sessions = SESSIONS.lock();
        sessions
            .get(&session_id)
            .cloned()
            .ok_or_else(|| napi::Error::from_reason(format!("unknown session id {}", session_id)))?
    };
    let snapshot = session.metrics_snapshot();
    Ok(SessionMetrics {
        produced_items: snapshot.produced_items.try_into().unwrap_or(i64::MAX),
        consumed_items: snapshot.consumed_items.try_into().unwrap_or(i64::MAX),
        max_inflight_items: snapshot.max_inflight_items.try_into().unwrap_or(i64::MAX),
        errors: snapshot.errors.try_into().unwrap_or(i64::MAX),
        total_seen: snapshot.total_seen.try_into().unwrap_or(i64::MAX),
        index_flush_ms: snapshot.index_flush_ms.try_into().unwrap_or(i64::MAX),
        dirs_seen: snapshot.dirs_seen.try_into().unwrap_or(i64::MAX),
        files_seen: snapshot.files_seen.try_into().unwrap_or(i64::MAX),
        files_emitted: snapshot.files_emitted.try_into().unwrap_or(i64::MAX),
        files_skipped_by_index: snapshot
            .files_skipped_by_index
            .try_into()
            .unwrap_or(i64::MAX),
        dirs_skipped_by_fingerprint: snapshot
            .dirs_skipped_by_fingerprint
            .try_into()
            .unwrap_or(i64::MAX),
        bytes_hashed: snapshot.bytes_hashed.try_into().unwrap_or(i64::MAX),
        batches_sent: snapshot.batches_sent.try_into().unwrap_or(i64::MAX),
        meta_queue_block_events: snapshot
            .meta_queue_block_events
            .try_into()
            .unwrap_or(i64::MAX),
        out_queue_block_events: snapshot
            .out_queue_block_events
            .try_into()
            .unwrap_or(i64::MAX),
        meta_queue_block_nanos: snapshot
            .meta_queue_block_nanos
            .try_into()
            .unwrap_or(i64::MAX),
        out_queue_block_nanos: snapshot
            .out_queue_block_nanos
            .try_into()
            .unwrap_or(i64::MAX),
        next_batch_calls: snapshot.next_batch_calls.try_into().unwrap_or(i64::MAX),
        finished: snapshot.finished,
    })
}

#[napi]
pub fn get_stats(session_id: u32) -> napi::Result<SessionMetrics> {
    session_metrics(session_id)
}
