use anyhow::Result;
use nouzdb::{DatabaseBuilder, Map};

fn main() -> Result<()> {
    let mut db = DatabaseBuilder::default().open("data/")?;
    db.set("hello", "world")?;
    let value = db.get(b"hello")?.unwrap();
    assert_eq!(value.as_ref().as_ref(), b"world");
    Ok(())
}
