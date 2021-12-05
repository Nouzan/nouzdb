use crate::memtable::Tree;
use crate::{Get, MapError};
use bytes::Bytes;
use csv::{ByteRecord, Reader, ReaderBuilder, WriterBuilder};
use std::fs::{File, OpenOptions};
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
        Segment::try_from(path.as_ref().to_owned())
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

/// Segment.
#[derive(Debug)]
pub struct Segment {
    path: PathBuf,
}

impl Segment {
    pub(crate) fn move_to<P: AsRef<Path>>(&mut self, path: &P) -> Result<(), std::io::Error> {
        std::fs::rename(&self.path, path)?;
        self.path = path.as_ref().to_owned();
        Ok(())
    }

    pub(crate) fn to_reader(&self) -> Result<Reader<File>, std::io::Error> {
        Ok(ReaderBuilder::new()
            .has_headers(false)
            .from_path(&self.path)?)
    }

    pub(crate) fn remove(self) -> Result<(), std::io::Error> {
        std::fs::remove_file(&self.path)
    }
}

impl TryFrom<PathBuf> for Segment {
    type Error = std::io::Error;

    fn try_from(path: PathBuf) -> Result<Self, Self::Error> {
        Ok(Self { path })
    }
}

impl Get for Segment {
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<bytes::Bytes>>, MapError>
    where
        Q: ?Sized,
        Q: AsRef<[u8]>,
    {
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .from_path(&self.path)
            .map_err(std::io::Error::from)?;
        for record in reader.byte_records() {
            if let Ok(record) = record {
                if let Some((k, v)) = record_to_kv(&record) {
                    if k == key.as_ref() {
                        return Ok(Some(Arc::new(v)));
                    }
                }
            }
        }
        Ok(None)
    }
}
