use crate::memtable::Tree;
use csv::{ByteRecord, WriterBuilder};
use std::fs::OpenOptions;
use std::path::Path;
use std::sync::Arc;

/// Raw Segment.
pub struct RawSegment {
    freeze: Arc<Tree>,
}

impl RawSegment {
    /// Write to path.
    pub fn write_to_path<P: AsRef<Path>>(&self, path: &P) -> Result<(), std::io::Error> {
        let file = OpenOptions::new().create_new(true).write(true).open(path)?;
        let mut writer = WriterBuilder::new().has_headers(false).from_writer(file);
        for (key, value) in self.freeze.iter() {
            let mut record = ByteRecord::new();
            record.push_field(key);
            record.push_field(value);
            writer.write_byte_record(&record)?;
        }
        Ok(())
    }
}

impl From<Arc<Tree>> for RawSegment {
    fn from(freeze: Arc<Tree>) -> Self {
        Self { freeze }
    }
}
