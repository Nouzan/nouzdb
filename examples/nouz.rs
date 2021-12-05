use std::path::PathBuf;

use anyhow::Result;
use nouzdb::{DatabaseBuilder, Get, Map};
use rustyline::error::ReadlineError;
use structopt::StructOpt;

#[derive(Debug, StructOpt)]
struct Opt {
    #[structopt(parse(from_os_str), default_value = "data/")]
    path: PathBuf,

    #[structopt(long, short, default_value = "1048576")]
    switch_mem_size: usize,

    #[structopt(long, short, default_value = "3600")]
    merge_period_secs: u64,
}

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let opt = Opt::from_args_safe()?;
    let mut db = DatabaseBuilder::default()
        .switch_mem_size(opt.switch_mem_size)
        .merge_period(std::time::Duration::from_secs(opt.merge_period_secs))
        .open(&opt.path)?;
    let mut rl = rustyline::Editor::<()>::new();
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                let mut cmds = line.split_whitespace();
                if let Some(cmd) = cmds.next() {
                    match cmd {
                        "get" => {
                            if let Some(key) = cmds.next() {
                                match db.get(&key) {
                                    Ok(Some(value)) => match String::from_utf8(value.to_vec()) {
                                        Ok(s) => {
                                            println!("{}", s);
                                        }
                                        _ => {
                                            println!("{:?}", value);
                                        }
                                    },
                                    Ok(None) => {
                                        println!("No `value` is set for this `key`");
                                    }
                                    Err(err) => {
                                        println!("Get error: {}", err);
                                    }
                                }
                            } else {
                                println!("Need a `key` to perform `get`.");
                            }
                        }
                        "set" => {
                            let key = cmds.next();
                            let value = cmds.next();
                            match (key, value) {
                                (Some(key), Some(value)) => {
                                    if let Err(err) = db.set(key.to_string(), value.to_string()) {
                                        println!("Set error: {}", err);
                                    }
                                }
                                (Some(_), None) => {
                                    println!("Need a `value` to perfrom `set`.");
                                }
                                (None, None) => {
                                    println!("Need a `key` and a `value` to perfrom `set`.");
                                }
                                _ => {}
                            }
                        }
                        cmd => {
                            println!("Unknown command: {}", cmd);
                        }
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    Ok(())
}
