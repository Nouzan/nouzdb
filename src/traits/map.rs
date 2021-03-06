use crate::errors::MapError;
use bytes::Bytes;
use std::sync::Arc;

/// Get.
pub trait Get {
    /// Get the value corresponding to the given key.
    fn get<Q>(&self, key: &Q) -> Result<Option<Arc<Bytes>>, MapError>
    where
        Q: ?Sized,
        Q: AsRef<[u8]>;
}

/// A Map.
pub trait Map: Get {
    /// Set the value of the given key, overwritten the previous value if it exists.
    fn set<K: Into<Bytes>, V: Into<Bytes>>(&mut self, key: K, value: V) -> Result<(), MapError>;
}
