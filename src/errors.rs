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

    /// Io errors.
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    /// Read lock error.
    #[error("read lock error")]
    ReadLock,

    /// Write lock error.
    #[error("write lock error")]
    WriteLock,
}
