//! The [`Database`] structure.

use crate::errors::MapError;
use crate::memtable::Memtable;
use crate::traits::Map;
use bytes::Bytes;
use std::{fs::DirBuilder, path::Path};
use thiserror::Error;

/// All errors of [`Database`]
#[derive(Debug, Error)]
pub enum Error {
    /// Io errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
}

/// A [`Database`] instance.
#[derive(Debug)]
pub struct Database {
    data_dir: String,
    memtable: Memtable,
}

impl Database {
    /// Create a new [`Database`] with a data folder path.
    pub fn new(data_dir: &str, log_file: &str) -> Result<Self, Error> {
        let path = Path::new(data_dir);
        DirBuilder::new().recursive(true).create(path)?;
        let data_dir = data_dir.to_string();
        let memtable = Memtable::from_path(path.join(log_file))?;
        Ok(Self { data_dir, memtable })
    }
}

impl Map for Database {
    fn get<Q>(&self, key: &Q) -> Result<&[u8], MapError>
    where
        Q: AsRef<[u8]>,
    {
        self.memtable.get(key)
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        self.memtable.set(key, value)
    }
}
