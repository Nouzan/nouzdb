//! The [`Database`] structure.

use crate::errors::MapError;
use crate::memtable::Memtable;
pub use crate::memtable::MemtableError;
use crate::segment::RawSegment;
use crate::traits::Map;
use bytes::Bytes;
use csv::{ByteRecord, ReaderBuilder};
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;
use std::{ffi::OsString, fs::DirBuilder, path::Path};
use thiserror::Error;

/// All errors of [`Database`]
#[derive(Debug, Error)]
pub enum Error {
    /// Io errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Invalid log file name.
    #[error("invalid log file name: {0:?}")]
    InvalidLogFileName(OsString),

    /// Memtable errors.
    #[error(transparent)]
    Memtable(#[from] MemtableError),

    /// Parse segment id error.
    #[error("error parsing {0} into segment id")]
    ParseSegemntId(String),
}

const DOT: char = '.';

/// A [`Database`] instance.
pub struct Database {
    data_dir: PathBuf,
    data_suffix: String,
    memtable: Arc<RwLock<Memtable>>,
    segments: Arc<RwLock<BTreeMap<u64, PathBuf>>>,
    max_segment_id: u64,
    tasks: Vec<thread::JoinHandle<Result<(), std::io::Error>>>,
}

impl Database {
    /// Create a new [`Database`] with a data folder path.
    pub fn new(
        dir: &str,
        log_suffix: &str,
        data_suffix: &str,
        switch_mem_size: usize,
    ) -> Result<Self, Error> {
        let path = Path::new(dir);
        DirBuilder::new().recursive(true).create(path)?;

        let mut logs = BTreeMap::new();
        let mut segments = BTreeMap::new();

        for entry in path.read_dir()? {
            if let Ok(entry) = entry {
                if let Some((id, suffix)) = entry
                    .file_name()
                    .into_string()
                    .map_err(|s| Error::InvalidLogFileName(s))?
                    .rsplit_once(DOT)
                {
                    if suffix == log_suffix {
                        logs.insert(id.to_string(), entry.path());
                    } else if suffix == data_suffix {
                        let id = id
                            .parse()
                            .map_err(|_| Error::ParseSegemntId(id.to_string()))?;
                        segments.insert(id, entry.path());
                    }
                }
            }
        }
        let max_segment_id: u64 = segments
            .iter()
            .next_back()
            .map(|(id, _)| *id)
            .unwrap_or_default();
        let data_dir = path.to_owned();
        let data_suffix = data_suffix.to_string();
        let (memtable, segment) = Memtable::new(logs, dir, log_suffix, switch_mem_size)?;
        let memtable = Arc::new(RwLock::new(memtable));
        let segments = Arc::new(RwLock::new(segments));
        let mut db = Self {
            data_dir,
            memtable,
            data_suffix,
            segments,
            max_segment_id,
            tasks: Vec::new(),
        };
        if let Some(segment) = segment {
            db.write_segment(segment)?;
        }
        Ok(db)
    }

    /// Force close.
    pub fn force_close(&mut self) {
        self.tasks.clear();
    }

    fn record_to_kv(record: &ByteRecord) -> Option<(&[u8], Bytes)> {
        let key = record.get(0)?;
        let value = Bytes::copy_from_slice(record.get(1)?);
        Some((key, value))
    }

    fn write_segment(&mut self, segment: RawSegment) -> Result<(), std::io::Error> {
        self.max_segment_id += 1;
        let segment_id = self.max_segment_id;
        let memtable = self.memtable.clone();
        let segments = self.segments.clone();
        let path = self
            .data_dir
            .as_path()
            .join(format!("{}.{}", segment_id, self.data_suffix));
        let tmp_path = self.data_dir.as_path().join(format!("{}.tmp", segment_id));
        let task = thread::spawn(move || -> Result<(), std::io::Error> {
            tracing::info!("writing new segment {} to path {:?}", segment_id, tmp_path);
            segment.write_to_path(&tmp_path)?;
            std::fs::rename(&tmp_path, &path)?;
            tracing::info!("new segment {} is written to path {:?}", segment_id, path);
            memtable.write().unwrap().finalize_switch()?;
            segments.write().unwrap().insert(segment_id, path);
            Ok(())
        });
        self.tasks.push(task);
        Ok(())
    }

    fn get_from_segments<Q>(&self, key: &Q) -> Result<Option<Bytes>, MapError>
    where
        Q: AsRef<[u8]>,
    {
        for (_, path) in self
            .segments
            .read()
            .map_err(|_| MapError::ReadLock)?
            .iter()
            .rev()
        {
            let mut reader = ReaderBuilder::new()
                .has_headers(false)
                .from_path(path)
                .map_err(std::io::Error::from)?;
            for record in reader.byte_records() {
                if let Ok(record) = record {
                    if let Some((k, v)) = Self::record_to_kv(&record) {
                        if k == key.as_ref() {
                            return Ok(Some(v));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}

impl Map for Database {
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<Bytes>>, MapError>
    where
        Q: AsRef<[u8]>,
    {
        if let Some(value) = self
            .memtable
            .read()
            .map_err(|_| MapError::ReadLock)?
            .get(key)?
        {
            Ok(Some(value))
        } else {
            Ok(self.get_from_segments(key)?.map(Arc::new))
        }
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        if let Some(segment) = {
            let mut write = self.memtable.write().map_err(|_| MapError::WriteLock)?;
            write.set(key, value)?;
            write.try_switch()?
        } {
            self.write_segment(segment)?;
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        for task in self.tasks.drain(..) {
            let _ = task.join();
        }
        tracing::info!("database closed");
    }
}
