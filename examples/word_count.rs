use anyhow::Result;
use nouzdb::{DatabaseBuilder, Map};
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use structopt::StructOpt;

/// Word count using `nouzdb`
#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str))]
    dir: PathBuf,
}

fn parse_to_usize(bytes: &[u8]) -> Result<usize> {
    Ok(String::from_utf8(bytes.to_vec())?.parse()?)
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args();
    let mut db = DatabaseBuilder::default().open("data/")?;
    for entry in opt.dir.read_dir()? {
        let path = entry?.path();
        if path.is_file() {
            let file = File::open(&path)?;
            for line in BufReader::new(file).lines() {
                for word in line?.split_whitespace() {
                    let mut count = db
                        .get(word)?
                        .and_then(|bytes| parse_to_usize(&bytes).ok())
                        .unwrap_or_default();
                    count += 1;
                    db.set(word.to_string(), count.to_string())?;
                }
            }
        }
    }
    Ok(())
}
