//! The [`Database`] structure.

use crate::errors::MapError;
use crate::memtable::Memtable;
pub use crate::memtable::MemtableError;
use crate::segment::{RawSegment, Segment};
use crate::traits::Map;
use crate::Get;
use bytes::Bytes;
use csv::WriterBuilder;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::path::PathBuf;

use std::sync::{mpsc, Arc, Mutex, RwLock};
use std::thread;
use std::time::Instant;
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
const TMP_SUFFIX: &str = "tmp";

type Segments = BTreeMap<u64, Segment>;

/// A [`Database`] instance.
pub struct Database {
    block_size: u64,
    merge_period: std::time::Duration,
    poll_period: std::time::Duration,
    data_dir: PathBuf,
    data_suffix: String,
    exiter: Option<mpsc::Sender<()>>,
    memtable: Arc<RwLock<Memtable>>,
    segments: Arc<RwLock<Segments>>,
    max_segment_id: Arc<Mutex<u64>>,
    tasks: Vec<thread::JoinHandle<Result<(), std::io::Error>>>,
}

impl Database {
    /// Create a new [`Database`] with a data folder path.
    pub(crate) fn new(
        path: &Path,
        log_suffix: &str,
        data_suffix: &str,
        switch_mem_size: usize,
        merge_period: std::time::Duration,
        poll_period: std::time::Duration,
        block_size: u64,
    ) -> Result<Self, Error> {
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
                        let mut segment = Segment::from_path(&entry.path());
                        segment.initialize_index(block_size)?;
                        segments.insert(id, segment);
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
        let (memtable, segment) = Memtable::new(logs, path, log_suffix, switch_mem_size)?;
        let memtable = Arc::new(RwLock::new(memtable));
        let segments = Arc::new(RwLock::new(segments));
        let mut db = Self {
            block_size,
            exiter: None,
            data_dir,
            memtable,
            data_suffix,
            segments,
            max_segment_id: Arc::new(Mutex::new(max_segment_id)),
            tasks: Vec::new(),
            merge_period,
            poll_period,
        };
        if let Some(segment) = segment {
            db.write_new_segment(segment)?;
        }
        db.start_merging_task();
        Ok(db)
    }

    fn start_merging_task(&mut self) {
        let (tx, rx) = mpsc::channel();
        let max_segment_id = self.max_segment_id.clone();
        let segments = self.segments.clone();
        let dir = self.data_dir.clone();
        let suffix = self.data_suffix.clone();
        let merge_period = self.merge_period;
        let poll_period = self.poll_period;
        let block_size = self.block_size;
        let task = thread::spawn(move || -> Result<(), std::io::Error> {
            Self::merge_segments(
                block_size,
                merge_period,
                poll_period,
                rx,
                max_segment_id,
                segments,
                dir,
                suffix,
            )
        });
        self.exiter = Some(tx);
        self.tasks.push(task);
    }

    /// Force close.
    pub fn force_close(&mut self) {
        if let Some(exiter) = self.exiter.take() {
            let _ = exiter.send(());
        }
        self.tasks.clear();
    }

    fn write_new_segment(&mut self, segment: RawSegment) -> Result<(), std::io::Error> {
        let memtable = self.memtable.clone();
        let segments = self.segments.clone();
        let dir = self.data_dir.clone();
        let suffix = self.data_suffix.clone();
        let max_segment_id = self.max_segment_id.clone();
        let block_size = self.block_size;
        let task = thread::spawn(move || -> Result<(), std::io::Error> {
            let mut segment_id = max_segment_id.lock().unwrap();
            *segment_id += 1;
            let path = dir
                .as_path()
                .join(format!("{}{}{}", segment_id, DOT, suffix));
            let tmp_path = dir
                .as_path()
                .join(format!("{}{}{}", segment_id, DOT, TMP_SUFFIX));
            tracing::info!("writing new segment {} to path {:?}", segment_id, tmp_path);
            let mut segment = segment.write_to_path(&tmp_path)?;
            segment.initialize_index(block_size)?;
            segment.move_to(&path)?;
            tracing::info!("new segment {} is written to path {:?}", segment_id, path);
            memtable.write().unwrap().finalize_switch()?;
            segments.write().unwrap().insert(*segment_id, segment);
            Ok(())
        });
        self.tasks.push(task);
        Ok(())
    }

    fn get_from_segments<Q>(&self, key: &Q) -> Result<Option<Arc<Bytes>>, MapError>
    where
        Q: ?Sized,
        Q: AsRef<[u8]>,
    {
        for (_, segment) in self
            .segments
            .read()
            .map_err(|_| MapError::ReadLock)?
            .iter()
            .rev()
        {
            if let Some(value) = segment.get(key)? {
                return Ok(Some(value));
            }
        }
        Ok(None)
    }

    fn merge_segments(
        block_size: u64,
        merge_period: std::time::Duration,
        poll_period: std::time::Duration,
        exiter: mpsc::Receiver<()>,
        max_segment_id: Arc<Mutex<u64>>,
        segments: Arc<RwLock<Segments>>,
        dir: PathBuf,
        suffix: String,
    ) -> Result<(), std::io::Error> {
        let mut last_tick = Instant::now();
        loop {
            thread::sleep(poll_period);
            match exiter.try_recv() {
                Ok(()) | Err(mpsc::TryRecvError::Disconnected) => {
                    break;
                }
                Err(_) => {
                    if last_tick.elapsed() >= merge_period {
                        if segments.read().unwrap().len() <= 1 {
                            continue;
                        }
                        let mut segment_id = max_segment_id.lock().unwrap();
                        *segment_id += 1;
                        last_tick = Instant::now();
                        let mut segment_readers = BTreeMap::new();
                        let mut failed = false;
                        for (id, segment) in segments.read().unwrap().iter() {
                            if let Ok(reader) = segment.to_reader() {
                                segment_readers.insert(*id, reader);
                            } else {
                                failed = true;
                                break;
                            }
                        }
                        if !failed {
                            let path = dir
                                .as_path()
                                .join(format!("{}{}{}", segment_id, DOT, suffix));
                            let tmp_path = dir
                                .as_path()
                                .join(format!("{}{}{}", segment_id, DOT, TMP_SUFFIX));
                            let mut failed = false;
                            if let Ok(tmp_file) =
                                OpenOptions::new().create(true).write(true).open(&tmp_path)
                            {
                                let mut writer = WriterBuilder::new()
                                    .has_headers(false)
                                    .from_writer(tmp_file);
                                tracing::info!("merging segments to to path {:?}", tmp_path);
                                let mut segment_records = segment_readers
                                    .iter_mut()
                                    .map(|(id, reader)| (id, reader.byte_records().peekable()))
                                    .collect::<BTreeMap<_, _>>();
                                loop {
                                    let mut done = Vec::new();
                                    let mut smallest = None;
                                    for (id, segment) in segment_records.iter_mut().rev() {
                                        if let Some(record) = segment.peek() {
                                            if let Some(key) = record
                                                .as_ref()
                                                .ok()
                                                .and_then(|record| record.get(0))
                                            {
                                                if let Some((_, smallest_key)) = smallest.as_ref() {
                                                    if key < smallest_key {
                                                        smallest = Some((
                                                            **id,
                                                            Bytes::copy_from_slice(key),
                                                        ));
                                                    } else if key == smallest_key {
                                                        segment.next();
                                                    }
                                                } else {
                                                    smallest =
                                                        Some((**id, Bytes::copy_from_slice(key)));
                                                }
                                            } else {
                                                segment.next();
                                            }
                                        } else {
                                            done.push(**id);
                                        }
                                    }
                                    if let Some((smallest_id, _)) = smallest {
                                        if let Some(record) = segment_records
                                            .get_mut(&smallest_id)
                                            .and_then(|record| record.next())
                                            .and_then(|record| record.ok())
                                        {
                                            if writer.write_byte_record(&record).is_err() {
                                                failed = true;
                                                break;
                                            }
                                        }
                                    } else {
                                        break;
                                    }
                                    for id in done {
                                        segment_records.remove(&id);
                                    }
                                }
                                if !failed {
                                    let mut segment = Segment::from_path(&tmp_path);
                                    match segment.initialize_index(block_size) {
                                        Ok(_) => {
                                            if let Err(err) = segment.move_to(&path) {
                                                tracing::error!(
                                                    "failed to rename the merged segment file: err={}",
                                                    err
                                                );
                                            } else {
                                                for id in segment_readers.keys() {
                                                    if let Some(old_segment) =
                                                        segments.write().unwrap().remove(id)
                                                    {
                                                        if let Err(err) = old_segment.remove() {
                                                            tracing::error!("failed to remove the old segment file in path {:?}, err={}", path, err);
                                                        }
                                                    }
                                                }
                                                tracing::info!(
                                                    "merged segments to to path {:?}",
                                                    path
                                                );
                                                segments
                                                    .write()
                                                    .unwrap()
                                                    .insert(*segment_id, segment);
                                            }
                                        }
                                        Err(err) => {
                                            tracing::error!(
                                                "failed to initialize the merged segment: err={}",
                                                err
                                            );
                                        }
                                    }
                                }
                                if tmp_path.exists() {
                                    let _ = std::fs::remove_file(&tmp_path);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

impl Get for Database {
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<Bytes>>, MapError>
    where
        Q: ?Sized,
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
            Ok(self.get_from_segments(key)?)
        }
    }
}

impl Map for Database {
    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        if let Some(segment) = {
            let mut write = self.memtable.write().map_err(|_| MapError::WriteLock)?;
            write.set(key, value)?;
            write.try_switch()?
        } {
            self.write_new_segment(segment)?;
        }
        Ok(())
    }
}

impl Drop for Database {
    fn drop(&mut self) {
        if let Some(exiter) = self.exiter.take() {
            let _ = exiter.send(());
        }
        for task in self.tasks.drain(..) {
            let _ = task.join();
        }
        if let Ok(mut segment_id) = self.max_segment_id.try_lock() {
            *segment_id += 1;
            if let Ok(mut memtable) = self.memtable.try_write() {
                if let Some(segment) = memtable.to_raw_segment() {
                    if segment.is_empty() {
                        let _ = memtable.remove_active_log();
                    } else {
                        let tmp_path = self
                            .data_dir
                            .as_path()
                            .join(format!("{}{}{}", *segment_id, DOT, TMP_SUFFIX));
                        let path = self
                            .data_dir
                            .as_path()
                            .join(format!("{}{}{}", *segment_id, DOT, self.data_suffix));
                        if segment.write_to_path(&tmp_path).is_ok() {
                            match std::fs::rename(&tmp_path, &path) {
                                Ok(_) => {
                                    let _ = memtable.remove_active_log();
                                    tracing::info!("created new segment file at path: {:?}", path);
                                }
                                Err(err) => {
                                    tracing::error!("rename temp segment file error: err={}", err);
                                }
                            }
                        }
                    }
                }
            }
        }
        tracing::info!("database closed");
    }
}
