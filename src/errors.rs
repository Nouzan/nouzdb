//! All error types.

use thiserror::Error;

/// [`Map`] operations errors.
#[derive(Debug, Error)]
pub enum MapError {
    /// The given key does not exist.
    #[error("key is missing")]
    KeyMissing,
}
