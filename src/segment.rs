use crate::memtable::Tree;
use crate::{Get, MapError};
use bytes::Bytes;
use csv::{ByteRecord, Reader, ReaderBuilder, WriterBuilder};
use std::collections::BTreeMap;
use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Raw Segment.
pub struct RawSegment {
    freeze: Arc<Tree>,
}

impl RawSegment {
    /// Write to path.
    pub fn write_to_path<P: AsRef<Path>>(&self, path: &P) -> Result<Segment, std::io::Error> {
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
        for (key, value) in self.freeze.iter() {
            let mut record = ByteRecord::new();
            record.push_field(key);
            record.push_field(value);
            writer.write_byte_record(&record)?;
        }
        Ok(Segment::from_path(path))
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.freeze.is_empty()
    }
}

impl From<Arc<Tree>> for RawSegment {
    fn from(freeze: Arc<Tree>) -> Self {
        Self { freeze }
    }
}

pub(crate) fn record_to_kv(record: &ByteRecord) -> Option<(&[u8], Bytes)> {
    let key = record.get(0)?;
    let value = Bytes::copy_from_slice(record.get(1)?);
    Some((key, value))
}

pub(crate) fn record_to_key(record: &ByteRecord) -> Option<Bytes> {
    let key = record.get(0)?;
    Some(Bytes::copy_from_slice(key))
}

/// Segment.
#[derive(Debug)]
pub struct Segment {
    index: Option<BTreeMap<Bytes, u64>>,
    path: PathBuf,
}

impl Segment {
    pub(crate) fn from_path<P: AsRef<Path>>(path: &P) -> Self {
        Self {
            path: path.as_ref().to_owned(),
            index: None,
        }
    }

    pub(crate) fn initialize_index(&mut self, block_size: u64) -> Result<(), std::io::Error> {
        let mut record = ByteRecord::new();
        let mut reader = self.to_reader()?;
        let mut index = BTreeMap::new();
        let mut last_block_offset = 0;
        let mut offset = 0;
        loop {
            let flag = offset == 57340;
            offset = reader.position().byte();
            tracing::debug!("offset: {}", offset);
            let more = reader.read_byte_record(&mut record)?;
            if flag {
                println!("{:?}", record);
            }
            if offset - last_block_offset >= block_size {
                last_block_offset = offset;
                if let Some(key) = record_to_key(&record) {
                    tracing::debug!("key: {:?}", key);
                    index.insert(key, offset);
                } else {
                    tracing::debug!("not key in this record");
                }
            }
            if !more {
                break;
            }
        }
        self.index = Some(index);
        Ok(())
    }

    pub(crate) fn move_to<P: AsRef<Path>>(&mut self, path: &P) -> Result<(), std::io::Error> {
        std::fs::rename(&self.path, path)?;
        self.path = path.as_ref().to_owned();
        Ok(())
    }

    pub(crate) fn to_reader(&self) -> Result<Reader<File>, std::io::Error> {
        Ok(ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_path(&self.path)?)
    }

    pub(crate) fn records(
        &self,
        start: u64,
    ) -> Result<impl Iterator<Item = Result<ByteRecord, std::io::Error>>, std::io::Error> {
        let mut file = File::open(&self.path)?;
        file.seek(SeekFrom::Start(start))?;
        let reader = ReaderBuilder::new().has_headers(false).from_reader(file);
        Ok(reader
            .into_byte_records()
            .map(|res| res.map_err(std::io::Error::from)))
    }

    pub(crate) fn remove(self) -> Result<(), std::io::Error> {
        std::fs::remove_file(&self.path)
    }
}

impl Get for Segment {
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<bytes::Bytes>>, MapError>
    where
        Q: ?Sized,
        Q: AsRef<[u8]>,
    {
        let offset = if let Some(index) = self.index.as_ref() {
            index
                .iter()
                .rev()
                .find(|(k, _)| *k <= key.as_ref())
                .map(|(_, p)| *p)
        } else {
            Some(0)
        };
        if let Some(offset) = offset {
            for record in self.records(offset)? {
                if let Ok(record) = record {
                    if let Some((k, v)) = record_to_kv(&record) {
                        if k == key.as_ref() {
                            return Ok(Some(Arc::new(v)));
                        }
                    }
                }
            }
        }
        Ok(None)
    }
}
