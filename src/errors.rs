//! All error types.

use thiserror::Error;

/// [`Map`] operations errors.
#[derive(Debug, Error)]
pub enum MapError {
    /// Write log error.
    #[error("write log error")]
    WriteLog,

    /// Key is not allowed.
    #[error("key is not allowed")]
    KeyNotAllow,
}
