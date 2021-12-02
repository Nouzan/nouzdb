use anyhow::Result;
use nouzdb::{Database, Map};
use rustyline::error::ReadlineError;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let mut db = Database::new("data/", "log", "data", 10)?;
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
