use crate::{Map, MapError};
use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_AIXM};
use csv::{ByteRecord, ReaderBuilder, Writer, WriterBuilder};
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::{collections::BTreeMap, fs::File};

/// Memtable.
pub struct Memtable {
    log: Writer<File>,
    active_tree: BTreeMap<Bytes, Bytes>,
    freeze_tree: Option<Arc<BTreeMap<Bytes, Bytes>>>,
    crc: Crc<u32>,
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
    ) -> Result<(BTreeMap<Bytes, Bytes>, u64), std::io::Error> {
        let mut tree = BTreeMap::new();
        let mut next_pos = 0;
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
                            tree.insert(key, value);
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
                        eprintln!("read record error: {}", err);
                    }
                }
            }
        }
        Ok((tree, next_pos))
    }

    pub fn new<P: AsRef<Path>>(
        logs: BTreeMap<String, PathBuf>,
        log_dir: P,
        log_suffix: &str,
    ) -> Result<Self, std::io::Error> {
        let crc = Crc::<u32>::new(&CRC_32_AIXM);
        let mut logs = logs.into_iter();
        let mut active_tree = None;
        let mut freeze_tree = None;
        let mut log_file = None;
        while let Some((_, path)) = logs.next_back() {
            if active_tree.is_none() {
                let (tree, next_pos) = Self::build_tree_from_path(&crc, &path)?;
                active_tree = Some(tree);
                let mut file = OpenOptions::new().create(true).write(true).open(path)?;
                file.seek(std::io::SeekFrom::Start(next_pos))?;
                file.set_len(next_pos)?;
                log_file = Some(file);
            } else if freeze_tree.is_none() {
                let (tree, _) = Self::build_tree_from_path(&crc, &path)?;
                freeze_tree = Some(Arc::new(tree));
            } else {
                let _ = std::fs::remove_file(path);
            }
        }
        let file = if let Some(log_file) = log_file {
            log_file
        } else {
            let path = log_dir.as_ref().join(format!("1.{}", log_suffix));
            OpenOptions::new().create(true).write(true).open(path)?
        };
        let log = WriterBuilder::new().has_headers(false).from_writer(file);
        let active_tree = active_tree.unwrap_or_default();
        Ok(Self {
            log,
            active_tree,
            crc,
            freeze_tree,
        })
    }
}

impl Map for Memtable {
    fn get<Q>(&self, key: &Q) -> Result<Option<&[u8]>, MapError>
    where
        Q: AsRef<[u8]>,
    {
        if let Some(value) = self.active_tree.get(key.as_ref()) {
            Ok(Some(value.as_ref()))
        } else if let Some(value) = self
            .freeze_tree
            .as_ref()
            .and_then(|tree| tree.get(key.as_ref()))
        {
            Ok(Some(value.as_ref()))
        } else {
            Ok(None)
        }
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        let key = key.into();
        let value = value.into();
        let record = self.parse_record(&key, &value);
        self.log
            .write_record(&record)
            .map_err(|_| MapError::WriteLog)?;
        self.log.flush().map_err(|_| MapError::WriteLog)?;
        self.active_tree.insert(key, value);
        Ok(())
    }
}
