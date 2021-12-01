//! An embedded database for learning purpose that is based on SSTables.

#![deny(missing_docs)]

pub mod database;
pub mod errors;
mod memtable;
pub mod traits;

pub use database::Database;
pub use errors::MapError;
pub use traits::Map;
