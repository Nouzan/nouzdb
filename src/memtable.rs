use crate::segment::RawSegment;
use crate::{Get, Map, MapError};
use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_AIXM};
use csv::{ByteRecord, ReaderBuilder, Writer, WriterBuilder};
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{collections::BTreeMap, fs::File};
use thiserror::Error;

pub(crate) type Tree = BTreeMap<Bytes, Arc<Bytes>>;

/// Memtable Errors.
#[derive(Debug, Error)]
pub enum MemtableError {
    /// Io errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Parse log id error.
    #[error("error parsing {0} into log id")]
    ParseLogId(String),
}

/// Memtable.
pub struct Memtable {
    log: Writer<File>,
    active_tree: Tree,
    freeze_tree: Option<Arc<Tree>>,
    active_size: usize,
    active_log_id: u64,
    freeze_log_id: Option<u64>,

    crc: Crc<u32>,
    log_dir: PathBuf,
    log_suffix: String,
    switch_active_size: usize,
}

impl Memtable {
    fn read_record(crc: &Crc<u32>, record: &ByteRecord) -> Option<(Bytes, Bytes)> {
        let mut digest = crc.digest();
        let crc = Bytes::copy_from_slice(record.get(0)?).get_u32_le();
        let key = Bytes::copy_from_slice(record.get(1)?);
        let value = Bytes::copy_from_slice(record.get(2)?);
        digest.update(&key);
        digest.update(&value);
        let check = digest.finalize();
        if check == crc {
            Some((key, value))
        } else {
            None
        }
    }

    fn parse_record(&self, key: &[u8], value: &[u8]) -> ByteRecord {
        let mut digest = self.crc.digest();
        digest.update(key);
        digest.update(value);
        let crc = digest.finalize().to_le_bytes();
        let mut record = ByteRecord::from(vec![&crc]);
        record.push_field(key);
        record.push_field(value);
        record
    }

    fn build_tree_from_path<P: AsRef<Path>>(
        crc: &Crc<u32>,
        path: &P,
    ) -> Result<(Tree, u64, usize), std::io::Error> {
        let mut tree = BTreeMap::new();
        let mut next_pos = 0;
        let mut size = 0;
        if let Ok(mut reader) = ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_path(&path)
        {
            let mut record = ByteRecord::new();
            loop {
                match reader.read_byte_record(&mut record) {
                    Ok(more) => {
                        if let Some((key, value)) = Self::read_record(&crc, &record) {
                            let key_size = key.len();
                            let value_size = value.len();
                            if let Some(old_value) = tree.insert(key, Arc::new(value)) {
                                size -= old_value.len();
                            } else {
                                size += key_size;
                            }
                            size += value_size;
                            next_pos = reader.position().byte();
                        } else {
                            break;
                        }
                        if !more {
                            break;
                        }
                        record.clear();
                    }
                    Err(err) => {
                        tracing::error!("read record error: {}", err);
                    }
                }
            }
        }
        Ok((tree, next_pos, size))
    }

    pub fn new<P: AsRef<Path>>(
        logs: BTreeMap<String, PathBuf>,
        log_dir: P,
        log_suffix: &str,
        switch_mem_size: usize,
    ) -> Result<(Self, Option<RawSegment>), MemtableError> {
        let crc = Crc::<u32>::new(&CRC_32_AIXM);
        let mut logs = logs.into_iter();
        let mut active_tree = None;
        let mut freeze_tree = None;
        let mut log_file = None;
        let mut active_log_id = 1;
        let mut freeze_log_id = None;
        let mut active_size = 0;
        let mut segment = None;
        while let Some((id, path)) = logs.next_back() {
            if active_tree.is_none() {
                let log_id = id.parse().map_err(|_| MemtableError::ParseLogId(id))?;
                let (tree, next_pos, size) = Self::build_tree_from_path(&crc, &path)?;
                active_size = size;
                active_tree = Some(tree);
                let mut file = OpenOptions::new().create(true).write(true).open(path)?;
                file.seek(std::io::SeekFrom::Start(next_pos))?;
                file.set_len(next_pos)?;
                log_file = Some(file);
                active_log_id = log_id;
            } else if freeze_tree.is_none() {
                let log_id = id.parse().map_err(|_| MemtableError::ParseLogId(id))?;
                let (tree, _, _) = Self::build_tree_from_path(&crc, &path)?;
                let tree = Arc::new(tree);
                freeze_tree = Some(tree.clone());
                freeze_log_id = Some(log_id);
                segment = Some(RawSegment::from(tree));
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
        let file = if let Some(log_file) = log_file {
            log_file
        } else {
            let path = log_dir
                .as_ref()
                .join(format!("{}.{}", active_log_id, log_suffix));
            OpenOptions::new().create(true).write(true).open(path)?
        };
        let log = WriterBuilder::new().has_headers(false).from_writer(file);
        let active_tree = active_tree.unwrap_or_default();
        Ok((
            Self {
                active_size,
                log,
                active_tree,
                crc,
                freeze_tree,
                freeze_log_id,
                log_dir: log_dir.as_ref().to_owned(),
                log_suffix: log_suffix.to_string(),
                active_log_id,
                switch_active_size: switch_mem_size,
            },
            segment,
        ))
    }

    fn force_switch(&mut self) -> Result<RawSegment, std::io::Error> {
        self.freeze_log_id = Some(self.active_log_id);
        self.active_log_id += 1;
        let path = self
            .log_dir
            .as_path()
            .join(format!("{}.{}", self.active_log_id, self.log_suffix));
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let log = WriterBuilder::new().has_headers(false).from_writer(file);
        let mut active_tree = BTreeMap::new();
        std::mem::swap(&mut self.active_tree, &mut active_tree);
        let tree = Arc::new(active_tree);
        self.freeze_tree = Some(tree.clone());
        self.log = log;
        tracing::info!("swithced to new memtable {}.", self.active_log_id);
        Ok(RawSegment::from(tree))
    }

    pub(crate) fn try_switch(&mut self) -> Result<Option<RawSegment>, std::io::Error> {
        tracing::debug!(
            "active_size={} switch_size={}",
            self.active_size,
            self.switch_active_size
        );
        if self.active_size > self.switch_active_size && self.freeze_tree.is_none() {
            let segment = self.force_switch()?;
            Ok(Some(segment))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn finalize_switch(&mut self) -> Result<(), std::io::Error> {
        self.freeze_tree = None;
        if let Some(log_id) = self.freeze_log_id.take() {
            let path = self
                .log_dir
                .as_path()
                .join(format!("{}.{}", log_id, self.log_suffix));
            std::fs::remove_file(path)?;
            tracing::info!("removed the log for freeze memtable {}.", log_id);
        }
        Ok(())
    }

    pub(crate) fn to_raw_segment(&mut self) -> Option<RawSegment> {
        if self.freeze_tree.is_none() {
            let mut tree = Tree::new();
            std::mem::swap(&mut tree, &mut self.active_tree);
            Some(RawSegment::from(Arc::new(tree)))
        } else {
            None
        }
    }

    pub(crate) fn remove_active_log(&mut self) -> Result<bool, std::io::Error> {
        if self.active_tree.is_empty() {
            let path = self
                .log_dir
                .as_path()
                .join(format!("{}.{}", self.active_log_id, self.log_suffix));
            std::fs::remove_file(path)?;
            tracing::info!(
                "removed the log for active memtable {}.",
                self.active_log_id
            );
            Ok(true)
        } else {
            Ok(false)
        }
    }
}

impl Get for Memtable {
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<Bytes>>, MapError>
    where
        Q: ?Sized,
        Q: AsRef<[u8]>,
    {
        if let Some(value) = self.active_tree.get(key.as_ref()) {
            Ok(Some(value.clone()))
        } else if let Some(value) = self
            .freeze_tree
            .as_ref()
            .and_then(|tree| tree.get(key.as_ref()))
        {
            Ok(Some(value.clone()))
        } else {
            Ok(None)
        }
    }
}

impl Map for Memtable {
    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        let key = key.into();
        let value = value.into();
        let record = self.parse_record(&key, &value);
        self.log
            .write_record(&record)
            .map_err(|_| MapError::WriteLog)?;
        self.log.flush().map_err(|_| MapError::WriteLog)?;
        let key_size = key.len();
        let value_size = value.len();
        if let Some(old_value) = self.active_tree.insert(key, Arc::new(value)) {
            self.active_size -= old_value.len();
        } else {
            self.active_size += key_size;
        }
        self.active_size += value_size;
        Ok(())
    }
}
