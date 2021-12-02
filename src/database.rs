//! The [`Database`] structure.

use crate::errors::MapError;
use crate::memtable::Memtable;
use crate::traits::Map;
use bytes::Bytes;
use std::collections::BTreeMap;
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
}

const DOT: char = '.';

/// A [`Database`] instance.
pub struct Database {
    data_dir: String,
    data_suffix: String,
    memtable: Memtable,
}

impl Database {
    /// Create a new [`Database`] with a data folder path.
    pub fn new(dir: &str, log_suffix: &str, data_suffix: &str) -> Result<Self, Error> {
        let path = Path::new(dir);
        DirBuilder::new().recursive(true).create(path)?;

        let mut logs = BTreeMap::new();

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
                    }
                }
            }
        }

        let data_dir = dir.to_string();
        let data_suffix = data_suffix.to_string();
        let memtable = Memtable::new(logs, dir, log_suffix)?;
        Ok(Self {
            data_dir,
            memtable,
            data_suffix,
        })
    }
}

impl Map for Database {
    fn get<Q>(&self, key: &Q) -> Result<Option<&[u8]>, MapError>
    where
        Q: AsRef<[u8]>,
    {
        self.memtable.get(key)
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        self.memtable.set(key, value)
    }
}
