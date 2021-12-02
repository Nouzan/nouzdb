//! An embedded database for learning purpose that is based on SSTables.

#![deny(missing_docs)]

pub mod builder;
pub mod database;
pub mod errors;
mod memtable;
mod segment;
pub mod traits;

pub use builder::DatabaseBuilder;
pub use database::{Database, Error};
pub use errors::MapError;
pub use traits::Map;
