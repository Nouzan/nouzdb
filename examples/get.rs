use anyhow::{anyhow, Result};
use nouzdb::{Database, Map};
use std::env;

fn main() -> Result<()> {
    let db = Database::new("data/", "log.csv")?;
    let mut args = env::args().skip(1);
    let key = args.next().ok_or(anyhow!("missing key input"))?;
    println!("{}", String::from_utf8(db.get(&key)?.to_vec())?);
    Ok(())
}
