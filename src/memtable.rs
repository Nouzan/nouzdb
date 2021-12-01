use crate::{Map, MapError};
use csv::{ReaderBuilder, StringRecord, Writer, WriterBuilder};
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::Path;
use std::{collections::BTreeMap, fs::File};

/// Memtable.
#[derive(Debug)]
pub struct Memtable {
    log: Writer<File>,
    tree: BTreeMap<String, String>,
}

impl Memtable {
    fn parse_record(record: &StringRecord) -> Option<(String, String)> {
        let key = record.get(0)?.to_string();
        let value = record.get(1)?.to_string();
        Some((key, value))
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let mut reader = ReaderBuilder::new().has_headers(false).from_path(&path)?;
        let mut tree = BTreeMap::new();
        let mut next_pos = 0;
        let mut record = StringRecord::new();
        while reader.read_record(&mut record).is_ok() {
            if let Some((key, value)) = Self::parse_record(&record) {
                tree.insert(key, value);
                next_pos = reader.position().byte();
            } else {
                break;
            }
        }
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.seek(std::io::SeekFrom::Start(next_pos))?;
        file.set_len(next_pos)?;
        let log = WriterBuilder::new().has_headers(false).from_writer(file);
        Ok(Self { log, tree })
    }
}

impl Map for Memtable {
    fn get<Q>(&self, key: &Q) -> Result<&str, MapError>
    where
        Q: ?Sized,
        String: std::borrow::Borrow<Q>,
        Q: Ord,
    {
        self.tree
            .get(key)
            .ok_or(MapError::KeyMissing)
            .map(String::as_str)
    }

    fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) -> Result<(), MapError> {
        let key = key.to_string();
        let value = value.to_string();
        let record = StringRecord::from(vec![key.as_str(), value.as_str()]);
        self.log
            .write_record(&record)
            .map_err(|_| MapError::WriteLog)?;
        self.log.flush().map_err(|_| MapError::WriteLog)?;
        self.tree.insert(key, value);
        Ok(())
    }
}
