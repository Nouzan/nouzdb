use anyhow::{anyhow, Result};
use nouzdb::{Database, Map};
use std::env;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut db = Database::new("data/", "log", "data", 10)?;
    let mut args = env::args().skip(1);
    let key = args.next().ok_or(anyhow!("missing key input"))?;
    let value = args.next().ok_or(anyhow!("missing value input"))?;
    db.set(key, value)?;
    Ok(())
}
