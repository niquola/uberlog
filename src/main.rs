// Updated example from http://rosettacode.org/wiki/Hello_world/Web_server#Rust
// to work with Rust 1.0 beta


extern crate postgres;
extern crate postgres_binary_copy;
// extern crate curl;
extern crate flate2;
extern crate json;
extern crate streaming_iterator;

use postgres::{Connection, TlsMode};
use std::env;
// use curl::http;

use std::fs::File;
use std::io::prelude::*;
use std::io::{self, BufReader, Cursor, Read};
// use std::path::Path;
use flate2::read::GzDecoder;
// use flate2::read::MultiGzDecoder;
// use postgres_binary_copy::BinaryCopyReader;
// use streaming_iterator::StreamingIterator;

struct IteratorAsRead<I>
where
    I: Iterator,
{
    iter: I,
    cursor: Option<Cursor<I::Item>>,
}

impl<I> IteratorAsRead<I>
where
    I: Iterator,
{
    pub fn new<T>(iter: T) -> Self
    where
        T: IntoIterator<IntoIter = I, Item = I::Item>,
    {
        let mut iter = iter.into_iter();
        let cursor = iter.next().map(Cursor::new);
        IteratorAsRead { iter, cursor }
    }
}

impl<I> Read for IteratorAsRead<I>
where
    I: Iterator,
Cursor<I::Item>: Read,
{
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        while self.cursor.is_some() {
            let read = self.cursor.as_mut().unwrap().read(buf)?;
            if read > 0 {
                return Ok(read);
            }
            self.cursor = self.iter.next().map(Cursor::new);
        }
        Ok(0)
    }
}


fn var(key: &str) -> String {
    match env::var(key) {
        Ok(val) => return val,
        Err(_) => return "ups".to_string(),
    }
}

fn conn_str() -> String {
   println!("postgres://{}:{}@{}:{}/{}", var("PGUSER"), var("PGPASSWORD"), var("PGHOST"), var("PGPORT"), var("PGDATABASE"));
   return format!("postgres://{}:{}@{}:{}/{}", var("PGUSER"), var("PGPASSWORD"), var("PGHOST"), var("PGPORT"), var("PGDATABASE"));
}


fn test() {

    // let f = File::open("tmp/out.gzip").unwrap();
    // let reader = BufReader::new(f);
    // let gzip = GzDecoder::new(reader);
    // let greader = BufReader::new(gzip);
    // let stream = greader.lines();

    let conn = Connection::connect(conn_str(), TlsMode::None).unwrap();
    let f = File::open("/tmp/aidbox.test.ndjson.gz").unwrap();
    let reader = BufReader::new(f);
    let gzip = GzDecoder::new(reader);
    let greader = BufReader::new(gzip);

    let stream = greader.lines();

    let source = stream.map(
        |res| {
            let jsonstr =  res.ok().unwrap();
            let pres;
            if let Ok(res) = json::parse(&jsonstr) {
                // print!("{}\t{}\t{}\n", res["ts"], res["ev"], res.dump());
                pres = format!("{}\t{}\t{}\n", res["ts"], res["ev"], res.dump());
            } else {
                println!("Parse error {}\n\n", jsonstr);
                pres = "".to_string();
            }
            pres
        }
    );


    // for line in greader.lines() {
    //     let l = line.unwrap();
    //     if let Ok(res) = json::parse(&l) {
    //         println!("{} {}", count, res["ev"]);
    //     } else {
    //         println!("Parse error {}", l)
    //     }
    //     count = count + 1;
    // }

    let mut source = IteratorAsRead::new(source);
    // CSV ESCAPE E'\\\\'
    let stmt = conn.prepare("COPY logs (ts, ev, resource)  FROM STDIN CSV quote e'\\x01' DELIMITER e'\\t'").unwrap();
    stmt.copy_in(&[], &mut source).unwrap();

}

fn main() {
    test()
}


// let res = &conn.query("select 'hi'", &[]).unwrap();
// let row = res.get(0);
// let json: String = row.get(0);

// println!("RES: {}", json);


// let url = "https://aidbox.app/User?_format=yaml&__secret=jobanarot";
// let resp = http::handle()
//     .get(url)
//     .exec()
//     .unwrap_or_else(|e| {
//         panic!("Failed to get {}; error is {}", url, e);
//     });

// if resp.get_code() != 200 {
//     println!("Unable to handle HTTP response code {}", resp.get_code());
//     return;
// }

// let body = std::str::from_utf8(resp.get_body()).unwrap_or_else(|e| {
//     panic!("Failed to parse response from {}; error is {}", url, e);
// });

// println!("{}",body);
