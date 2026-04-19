#![allow(unused_imports)]
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::thread;

fn main() {
    println!("Logs from your program will appear here!");

    let listener = TcpListener::bind("127.0.0.1:6379").unwrap();

    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("accepted new connection");
                thread::spawn(move || handle_connection(stream));
            }
            Err(e) => {
                println!("error: {}", e);
            }
        }
    }
}

fn get_parts(buf: &[u8], n: usize) -> Vec<&str> {
    let data = &buf[..n];
    let text = std::str::from_utf8(data).unwrap();
    let parts: Vec<&str> = text.split("\r\n").collect();
    return parts;
}

fn handle_connection(mut stream: TcpStream) {
    let mut buf = [0u8; 512];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => return,
            Ok(n) => {
                let parts = get_parts(&buf, n);
                let command = parts.get(2).copied().unwrap_or("").to_ascii_uppercase();

                let response: Vec<u8> = match command.as_str() {
                    "ECHO" => {
                        let arg = parts.get(4).copied().unwrap_or("");
                        format!("${}\r\n{}\r\n", arg.len(), arg).into_bytes()
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
