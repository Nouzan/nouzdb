use crate::{Map, MapError};
use bytes::Bytes;
use csv::{ByteRecord, ReaderBuilder, Writer, WriterBuilder};
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::Path;
use std::{collections::BTreeMap, fs::File};

/// Memtable.
#[derive(Debug)]
pub struct Memtable {
    log: Writer<File>,
    tree: BTreeMap<Bytes, Bytes>,
}

impl Memtable {
    fn parse_record(record: &ByteRecord) -> Option<(Bytes, Bytes)> {
        let key = Bytes::copy_from_slice(record.get(0)?);
        let value = Bytes::copy_from_slice(record.get(1)?);
        Some((key, value))
    }

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let mut reader = ReaderBuilder::new().has_headers(false).from_path(&path)?;
        let mut tree = BTreeMap::new();
        let mut next_pos = 0;
        let mut record = ByteRecord::new();
        while reader.read_byte_record(&mut record).is_ok() {
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
    fn get<Q>(&self, key: &Q) -> Result<&[u8], MapError>
    where
        Q: AsRef<[u8]>,
    {
        self.tree
            .get(key.as_ref())
            .ok_or(MapError::KeyMissing)
            .map(Bytes::as_ref)
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        let key = key.into();
        let value = value.into();
        let record = ByteRecord::from(vec![key.as_ref(), value.as_ref()]);
        self.log
            .write_record(&record)
            .map_err(|_| MapError::WriteLog)?;
        self.log.flush().map_err(|_| MapError::WriteLog)?;
        self.tree.insert(key, value);
        Ok(())
    }
}
