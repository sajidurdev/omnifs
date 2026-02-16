use std::collections::HashMap;
use std::fs;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::stream::FileRecord;

const INDEX_FILE_NAME: &str = ".omnifs-index.jsonl";
#[cfg(not(windows))]
const INDEX_TMP_FILE_NAME: &str = ".omnifs-index.tmp";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HashMode {
    Disabled,
    Blake3,
}

impl HashMode {
    pub fn from_option(hash: Option<&str>) -> Self {
        match hash {
            Some("blake3") => Self::Blake3,
            _ => Self::Disabled,
        }
    }

    pub fn is_enabled(self) -> bool {
        self != Self::Disabled
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IndexRecord {
    pub path: String,
    pub size: u64,
    pub mtime_ms: u64,
    pub identity: String,
    pub hash: Option<String>,
}

impl From<&FileRecord> for IndexRecord {
    fn from(value: &FileRecord) -> Self {
        Self {
            path: value.path.clone(),
            size: value.size,
            mtime_ms: value.mtime_ms,
            identity: value.identity.clone(),
            hash: value.hash.clone(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DirFingerprintRecord {
    pub dir_path: String,
    pub dir_mtime_ms: u64,
    pub child_count: u64,
    pub name_hash: Option<String>,
    #[serde(default)]
    pub weak: bool,
}

impl DirFingerprintRecord {
    pub fn matches(&self, current: &DirFingerprintRecord) -> bool {
        if self.dir_mtime_ms != current.dir_mtime_ms || self.child_count != current.child_count {
            return false;
        }
        if self.weak || current.weak {
            return true;
        }
        self.name_hash == current.name_hash
    }
}

#[derive(Clone)]
pub struct IndexSnapshot {
    file_entries: Arc<HashMap<String, IndexRecord>>,
    dir_entries: Arc<HashMap<String, DirFingerprintRecord>>,
}

impl IndexSnapshot {
    pub fn is_file_unchanged(
        &self,
        relative_path: &str,
        size: u64,
        mtime_ms: u64,
        identity: &str,
    ) -> bool {
        let Some(existing) = self.file_entries.get(relative_path) else {
            return false;
        };
        existing.size == size && existing.mtime_ms == mtime_ms && existing.identity == identity
    }

    pub fn dir_fingerprint_matches(
        &self,
        relative_path: &str,
        current: &DirFingerprintRecord,
    ) -> bool {
        let Some(existing) = self.dir_entries.get(relative_path) else {
            return false;
        };
        existing.matches(current)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
enum StoredRecord {
    File {
        path: String,
        size: u64,
        mtime_ms: u64,
        identity: String,
        hash: Option<String>,
    },
    Dir {
        dir_path: String,
        dir_mtime_ms: u64,
        child_count: u64,
        name_hash: Option<String>,
        #[serde(default)]
        weak: bool,
    },
}

#[derive(Debug)]
pub struct IncrementalIndex {
    root: PathBuf,
    index_path: PathBuf,
    file_entries: HashMap<String, IndexRecord>,
    dir_entries: HashMap<String, DirFingerprintRecord>,
    dirty: bool,
}

impl IncrementalIndex {
    pub fn open(root: PathBuf) -> std::io::Result<Self> {
        let index_path = root.join(INDEX_FILE_NAME);
        let mut file_entries = HashMap::new();
        let mut dir_entries = HashMap::new();

        if index_path.exists() {
            let file = fs::File::open(&index_path)?;
            let reader = BufReader::new(file);
            for line in reader.lines() {
                let line = match line {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if line.trim().is_empty() {
                    continue;
                }
                if let Ok(stored) = serde_json::from_str::<StoredRecord>(&line) {
                    match stored {
                        StoredRecord::File {
                            path,
                            size,
                            mtime_ms,
                            identity,
                            hash,
                        } => {
                            file_entries.insert(
                                path.clone(),
                                IndexRecord {
                                    path,
                                    size,
                                    mtime_ms,
                                    identity,
                                    hash,
                                },
                            );
                        }
                        StoredRecord::Dir {
                            dir_path,
                            dir_mtime_ms,
                            child_count,
                            name_hash,
                            weak,
                        } => {
                            dir_entries.insert(
                                dir_path.clone(),
                                DirFingerprintRecord {
                                    dir_path,
                                    dir_mtime_ms,
                                    child_count,
                                    name_hash,
                                    weak,
                                },
                            );
                        }
                    }
                    continue;
                }

                // Backward compatibility with older JSONL file-only records.
                if let Ok(record) = serde_json::from_str::<IndexRecord>(&line) {
                    file_entries.insert(record.path.clone(), record);
                }
            }
        }

        Ok(Self {
            root,
            index_path,
            file_entries,
            dir_entries,
            dirty: false,
        })
    }

    pub fn snapshot(&self) -> IndexSnapshot {
        IndexSnapshot {
            file_entries: Arc::new(self.file_entries.clone()),
            dir_entries: Arc::new(self.dir_entries.clone()),
        }
    }

    pub fn should_emit_and_update(&mut self, file: &FileRecord) -> bool {
        let key = normalize_path(&self.root, Path::new(&file.path));
        let previous = self.file_entries.get(&key);
        let changed = match previous {
            None => true,
            Some(existing) => {
                existing.size != file.size
                    || existing.mtime_ms != file.mtime_ms
                    || existing.identity != file.identity
                    || existing.hash != file.hash
            }
        };

        if changed {
            let mut record = IndexRecord::from(file);
            record.path = key.clone();
            self.file_entries.insert(key, record);
            self.dirty = true;
        }

        changed
    }

    pub fn update_dir_fingerprint(&mut self, mut record: DirFingerprintRecord) {
        record.dir_path = normalize_relative_path(&record.dir_path);
        self.dir_entries.insert(record.dir_path.clone(), record);
        self.dirty = true;
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        if !self.dirty {
            return Ok(());
        }

        #[cfg(windows)]
        let tmp_path = self.index_path.clone();
        #[cfg(not(windows))]
        let tmp_path = self.root.join(INDEX_TMP_FILE_NAME);
        let file = fs::File::create(&tmp_path).map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("create temp index {}: {}", tmp_path.display(), err),
            )
        })?;
        let mut writer = BufWriter::new(file);

        let mut file_keys = self.file_entries.keys().cloned().collect::<Vec<_>>();
        file_keys.sort_unstable();
        for key in file_keys {
            let Some(record) = self.file_entries.get(&key) else {
                continue;
            };
            let line = serde_json::to_string(&StoredRecord::File {
                path: record.path.clone(),
                size: record.size,
                mtime_ms: record.mtime_ms,
                identity: record.identity.clone(),
                hash: record.hash.clone(),
            })
            .map_err(to_io_error)?;
            writer.write_all(line.as_bytes()).map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "write file record {} to {}: {}",
                        key,
                        tmp_path.display(),
                        err
                    ),
                )
            })?;
            writer.write_all(b"\n").map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "write file record newline {} to {}: {}",
                        key,
                        tmp_path.display(),
                        err
                    ),
                )
            })?;
        }

        let mut dir_keys = self.dir_entries.keys().cloned().collect::<Vec<_>>();
        dir_keys.sort_unstable();
        for key in dir_keys {
            let Some(record) = self.dir_entries.get(&key) else {
                continue;
            };
            let line = serde_json::to_string(&StoredRecord::Dir {
                dir_path: record.dir_path.clone(),
                dir_mtime_ms: record.dir_mtime_ms,
                child_count: record.child_count,
                name_hash: record.name_hash.clone(),
                weak: record.weak,
            })
            .map_err(to_io_error)?;
            writer.write_all(line.as_bytes()).map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "write dir record {} to {}: {}",
                        key,
                        tmp_path.display(),
                        err
                    ),
                )
            })?;
            writer.write_all(b"\n").map_err(|err| {
                std::io::Error::new(
                    err.kind(),
                    format!(
                        "write dir record newline {} to {}: {}",
                        key,
                        tmp_path.display(),
                        err
                    ),
                )
            })?;
        }

        writer.flush().map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!("flush temp index {}: {}", tmp_path.display(), err),
            )
        })?;
        drop(writer);
        #[cfg(not(windows))]
        replace_index_file(&tmp_path, &self.index_path).map_err(|err| {
            std::io::Error::new(
                err.kind(),
                format!(
                    "replace index {} -> {}: {}",
                    tmp_path.display(),
                    self.index_path.display(),
                    err
                ),
            )
        })?;
        self.dirty = false;
        Ok(())
    }
}

fn to_io_error(err: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, err.to_string())
}

pub fn normalize_path(root: &Path, path: &Path) -> String {
    path.strip_prefix(root)
        .unwrap_or(path)
        .to_string_lossy()
        .replace('\\', "/")
}

fn normalize_relative_path(path: &str) -> String {
    path.replace('\\', "/")
}

#[cfg(not(windows))]
fn replace_index_file(tmp_path: &Path, final_path: &Path) -> std::io::Result<()> {
    fs::rename(tmp_path, final_path)
}
