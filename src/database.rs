//! The [`Database`] structure.

use crate::errors::MapError;
use crate::traits::Map;
use bytes::Bytes;
use std::{collections::BTreeMap, path::Path};

/// A [`Database`] instance.
#[derive(Debug)]
pub struct Database {
    memtable: BTreeMap<Bytes, Bytes>,
}

impl Database {
    /// Create a new [`Database`] with a data folder path.
    pub fn new(_path: &Path) -> Self {
        Self {
            memtable: BTreeMap::new(),
        }
    }
}

impl Map for Database {
    fn get<Q>(&self, key: &Q) -> Result<&[u8], MapError>
    where
        Q: ?Sized,
        Bytes: std::borrow::Borrow<Q>,
        Q: Ord,
    {
        self.memtable
            .get(key)
            .ok_or(MapError::KeyMissing)
            .map(Bytes::as_ref)
    }

    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError> {
        self.memtable.insert(key.into(), value.into());
        Ok(())
    }
}
