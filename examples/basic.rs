use anyhow::Result;
use nouzdb::{Database, Map};

fn main() -> Result<()> {
    let mut db = Database::new("data/", "log", "data", 1024)?;
    db.set("hello", "world")?;
    let value = db.get(b"hello")?.unwrap();
    assert_eq!(value.as_ref().as_ref(), b"world");
    Ok(())
}
