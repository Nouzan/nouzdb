use anyhow::{anyhow, Result};
use nouzdb::{DatabaseBuilder, Map};
use std::env;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut db = DatabaseBuilder::default()
        .switch_mem_size(10)
        .open("data/")?;
    let mut args = env::args().skip(1);
    let key = args.next().ok_or(anyhow!("missing key input"))?;
    let value = args.next().ok_or(anyhow!("missing value input"))?;
    db.set(key, value)?;
    db.force_close();
    Ok(())
}
