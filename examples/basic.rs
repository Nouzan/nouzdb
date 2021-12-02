use anyhow::Result;
use nouzdb::{Database, Map};

fn main() -> Result<()> {
    let mut db = Database::new("data/", "log", "data")?;
    db.set("hello", "world")?;
    assert_eq!(db.get(b"hello")?.unwrap(), b"world");
    Ok(())
}
