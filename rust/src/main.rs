use crossbeam_channel::bounded;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::thread;

fn main() -> io::Result<()> {
    let file = File::open("../data/create_measurements.txt")?;
    let reader = BufReader::new(file);

    let mut lines_iter = reader.lines();
    let chunk_size = 1_000_000;
    let num_workers = 10;

    let (tx, rx) = bounded::<Vec<String>>(num_workers * 2);

    let mut handles = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let rx = rx.clone();
        handles.push(thread::spawn(move || {
            println!("Worker {} started", i);
            while let Ok(chunk) = rx.recv() {
                process_chunk(&chunk, i);
            }
            println!("Worker {} finished", i);
        }));
    }

    loop {
        let mut chunk: Vec<String> = Vec::with_capacity(chunk_size);
        for _ in 0..chunk_size {
            match lines_iter.next() {
                Some(Ok(line)) => chunk.push(line),
                Some(Err(e)) => return Err(e),
                None => break,
            }
        }
        if chunk.is_empty() {
            break;
        }
        tx.send(chunk).unwrap();
    }
    drop(tx);
    for h in handles {
        h.join().unwrap();
    }
    Ok(())
}
