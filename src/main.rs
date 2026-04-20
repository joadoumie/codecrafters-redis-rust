#![allow(unused_imports)]
use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

struct Value {
    data: String,
    expires_at: Option<Instant>,
}

type Db = Arc<Mutex<HashMap<String, Value>>>;
type List = Arc<Mutex<HashMap<String, Vec<String>>>>;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();
    let db: Db = Arc::new(Mutex::new(HashMap::new()));
    let list: List = Arc::new(Mutex::new(HashMap::new()));

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                let db = Arc::clone(&db);
                let list = Arc::clone(&list);
                thread::spawn(move || handle_connection(stream, db, list));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn get_parts(buf: &[u8], n: usize) -> Option<Vec<&str>> {
    let text = std::str::from_utf8(&buf[..n]).ok()?;
    Some(text.split("\r\n").collect())
}

fn handle_connection(mut stream: TcpStream, db: Db, list: List) {
    let mut buf = [0u8; 512];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                let parts = match get_parts(&buf, n) {
                    Some(p) => p,
                    None => {
                        if stream.write_all(b"-ERR invalid request\r\n").is_err() {
                            return;
                        }
                        continue;
                    }
                };
                let command = parts.get(2).copied().unwrap_or("").to_ascii_uppercase();

                let response: Vec<u8> = match command.as_str() {
                    "ECHO" => {
                        let arg = parts.get(4).copied().unwrap_or("");
                        format!("${}\r\n{}\r\n", arg.len(), arg).into_bytes()
                    }
                    "SET" => {
                        let key = parts.get(4).copied().unwrap_or("").to_string();
                        let data = parts.get(6).copied().unwrap_or("").to_string();

                        let expires_at = match parts
                            .get(8)
                            .copied()
                            .map(|s| s.to_ascii_uppercase())
                            .as_deref()
                        {
                            Some("PX") => parts
                                .get(10)
                                .and_then(|s| s.parse::<u64>().ok())
                                .map(|ms| Instant::now() + Duration::from_millis(ms)),
                            Some("EX") => parts
                                .get(10)
                                .and_then(|s| s.parse::<u64>().ok())
                                .map(|s| Instant::now() + Duration::from_secs(s)),
                            _ => None,
                        };

                        db.lock().unwrap().insert(key, Value { data, expires_at });
                        b"+OK\r\n".to_vec()
                    }
                    "GET" => {
                        let key = parts.get(4).copied().unwrap_or("");
                        let mut map = db.lock().unwrap();
                        match map.get(key) {
                            Some(v) if v.expires_at.is_some_and(|e| Instant::now() >= e) => {
                                map.remove(key);
                                b"$-1\r\n".to_vec()
                            }
                            Some(v) => format!("${}\r\n{}\r\n", v.data.len(), v.data).into_bytes(),
                            None => b"$-1\r\n".to_vec(),
                        }
                    }
                    "RPUSH" => {
                        let key = parts.get(4).copied().unwrap_or("").to_string();
                        let mut i = 6;
                        let mut map = list.lock().unwrap();
                        let entry = map.entry(key).or_default();
                        while i < parts.len() {
                            let data = parts.get(i).copied().unwrap_or("").to_string();
                            entry.push(data);
                            i += 2;
                        }

                        let len = entry.len();

                        format!(":{}\r\n", len).into_bytes()
                    }
                    _ => b"+PONG\r\n".to_vec(),
                };

                if stream.write_all(&response).is_err() {
                    return;
                }
            }
            Err(e) => {
                println!("read error: {}", e);
                return;
            }
        }
    }
}
