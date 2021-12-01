use std::path::Path;

use anyhow::Result;
use nouzdb::{Database, Map};

fn main() -> Result<()> {
    let path = Path::new("/var/nouzdb/data");
    let mut db = Database::new(path);
    db.set("hello", "world")?;
    assert_eq!(db.get("hello".as_bytes())?, b"world");
    Ok(())
}
