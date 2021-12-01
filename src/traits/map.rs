use std::borrow::Borrow;

use crate::errors::MapError;
use bytes::Bytes;

/// A Map.
pub trait Map {
    /// Get the value corresponding to the given key.
    fn get<Q>(&self, key: &Q) -> Result<&[u8], MapError>
    where
        Q: ?Sized,
        Bytes: Borrow<Q>,
        Q: Ord;

    /// Set the value of the given key, overwritten the previous value if it exists.
    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError>;
}