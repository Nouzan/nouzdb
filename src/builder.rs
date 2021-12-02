//! Builder for [`Database`].

use crate::{database::Error, Database};
use std::path::Path;

/// Default log suffix.
pub const DEFAULT_LOG_SUFFIX: &str = "log";
/// Default data suffix.
pub const DEFAULT_DATA_SUFIX: &str = "data";
/// Default switch mem size.
pub const DEFAULT_SWTICH_MEM_SIZE: usize = 1024 * 1024;
/// Default merge period in secs.
pub const DEFAULT_MERGE_PERIOD_SECS: u64 = 3600;
/// Default poll period in millis.
pub const DEFAULT_POLL_PERIOD_MILLIS: u64 = 100;

/// Database builder.
#[derive(Debug)]
pub struct DatabaseBuilder {
    log_suffix: String,
    data_suffix: String,
    switch_mem_size: usize,
    merge_period: std::time::Duration,
    poll_period: std::time::Duration,
}

impl Default for DatabaseBuilder {
    fn default() -> Self {
        Self {
            log_suffix: DEFAULT_LOG_SUFFIX.to_string(),
            data_suffix: DEFAULT_DATA_SUFIX.to_string(),
            switch_mem_size: DEFAULT_SWTICH_MEM_SIZE,
            merge_period: std::time::Duration::from_secs(DEFAULT_MERGE_PERIOD_SECS),
            poll_period: std::time::Duration::from_millis(DEFAULT_POLL_PERIOD_MILLIS),
        }
    }
}

impl DatabaseBuilder {
    /// Open database at `path`.
    pub fn open<P: AsRef<Path>>(&self, path: &P) -> Result<Database, Error>
    where
        P: ?Sized,
    {
        Database::new(
            path.as_ref(),
            &self.log_suffix,
            &self.data_suffix,
            self.switch_mem_size,
            self.merge_period,
            self.poll_period,
        )
    }

    /// Set log suffix.
    pub fn log_suffix(&mut self, suffix: &str) -> &mut Self {
        self.log_suffix = suffix.to_string();
        self
    }

    /// Set log suffix.
    pub fn data_suffix(&mut self, suffix: &str) -> &mut Self {
        self.data_suffix = suffix.to_string();
        self
    }

    /// Set switch mem size.
    pub fn switch_mem_size(&mut self, size: usize) -> &mut Self {
        self.switch_mem_size = size;
        self
    }

    /// Set merge period.
    pub fn merge_period(&mut self, duration: std::time::Duration) -> &mut Self {
        self.merge_period = duration;
        self
    }

    /// Set poll period.
    pub fn poll_period(&mut self, duration: std::time::Duration) -> &mut Self {
        self.poll_period = duration;
        self
    }
}
