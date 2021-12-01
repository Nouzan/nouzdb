use anyhow::Result;
use nouzdb::{Database, Map};

fn main() -> Result<()> {
    let mut db = Database::new("data/", "log.csv")?;
    db.set("hello", "world")?;
    assert_eq!(db.get(b"hello")?, b"world");
    Ok(())
}
