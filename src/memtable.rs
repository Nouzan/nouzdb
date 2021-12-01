use crate::{Map, MapError};
use bytes::{Buf, Bytes};
use crc::{Crc, CRC_32_AIXM};
use csv::{ByteRecord, ReaderBuilder, Writer, WriterBuilder};
use std::fs::OpenOptions;
use std::io::Seek;
use std::path::Path;
use std::{collections::BTreeMap, fs::File};

/// Memtable.
pub struct Memtable {
    log: Writer<File>,
    tree: BTreeMap<Bytes, Bytes>,
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

    pub fn from_path<P: AsRef<Path>>(path: P) -> Result<Self, std::io::Error> {
        let crc = Crc::<u32>::new(&CRC_32_AIXM);
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
        let mut file = OpenOptions::new().create(true).write(true).open(path)?;
        file.seek(std::io::SeekFrom::Start(next_pos))?;
        file.set_len(next_pos)?;
        let log = WriterBuilder::new().has_headers(false).from_writer(file);
        Ok(Self { log, tree, crc })
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
        let record = self.parse_record(&key, &value);
        self.log
            .write_record(&record)
            .map_err(|_| MapError::WriteLog)?;
        self.log.flush().map_err(|_| MapError::WriteLog)?;
        self.tree.insert(key, value);
        Ok(())
    }
}
