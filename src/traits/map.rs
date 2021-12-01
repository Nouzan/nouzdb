use std::borrow::Borrow;

use crate::errors::MapError;

/// A Map.
pub trait Map {
    /// Get the value corresponding to the given key.
    fn get<Q>(&self, key: &Q) -> Result<&str, MapError>
    where
        Q: ?Sized,
        String: Borrow<Q>,
        Q: Ord;

    /// Set the value of the given key, overwritten the previous value if it exists.
    fn set<K: ToString, V: ToString>(&mut self, key: K, value: V) -> Result<(), MapError>;
}
