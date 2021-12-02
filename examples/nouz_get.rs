use anyhow::{anyhow, Result};
use nouzdb::{Database, Map};
use std::env;

fn main() -> Result<()> {
    let db = Database::new("data/", "log", "data")?;
    let mut args = env::args().skip(1);
    let key = args.next().ok_or(anyhow!("missing key input"))?;
    if let Some(value) = db.get(&key)? {
        println!("{}", String::from_utf8(value.to_vec())?);
    } else {
        println!("No such key.")
    }
    Ok(())
}
