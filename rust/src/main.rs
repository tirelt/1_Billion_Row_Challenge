use crossbeam_channel::bounded;
use std::collections::HashMap;
use std::fs::File;
use std::io;
use std::io::{BufRead, BufReader};
use std::sync::mpsc;
use std::thread;

fn main() -> io::Result<()> {
    let file = File::open("../data/measurements.txt")?;
    let reader = BufReader::new(file);

    let mut lines_iter = reader.lines();
    let chunk_size = 1_000_000;
    let num_workers = 10;

    let (tx, rx) = bounded::<Vec<String>>(num_workers * 2);
    let (tx_res, rx_res) = mpsc::channel();

    let mut handles = Vec::with_capacity(num_workers);
    for i in 0..num_workers {
        let rx = rx.clone();
        let tx_res = tx_res.clone();
        let mut map: HashMap<String, (i32, i32, i32, i32)> = HashMap::new();
        handles.push(thread::spawn(move || {
            println!("Worker {} started", i);
            while let Ok(chunk) = rx.recv() {
                process_chunk(&chunk, &mut map);
            }
            let _ = tx_res.send(map);
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
    let mut maps = Vec::new();
    for _ in 0..num_workers {
        maps.push(rx_res.recv().unwrap());
    }
    maps.into_iter().fold(HashMap::new(), |mut acc, map| {
        for (k, v) in map {
            acc.entry(k.clone())
                .and_modify(|existing| aggregate(existing, v))
                .or_insert(v.clone());
        }
        acc
    });
    Ok(())
}

fn process_chunk(chunk: &Vec<String>, map: &mut HashMap<String, (i32, i32, i32, i32)>) {
    for line in chunk.iter() {
        let mut temp = 0;
        let mut counter = 0;
        let mut mult = 1;
        for c in line.chars().rev() {
            counter += 1;
            match c {
                x if x.is_digit(10) => {
                    temp += mult * x.to_digit(10).unwrap() as i32;
                    mult *= 10;
                }
                '-' => temp = -temp,
                ';' => break,
                '.' => (),
                _ => panic!("should not be here"),
            };
        }
        let city = line[..(line.len() - counter)].to_owned();
        map.entry(city)
            .and_modify(|existing| aggregate(existing, (temp, temp, temp, 1)))
            .or_insert((temp, temp, temp, 1));
    }
}

fn aggregate(
    (min, max, sum, count): &mut (i32, i32, i32, i32),
    (new_min, new_max, new_sum, new_count): (i32, i32, i32, i32),
) {
    if new_min < *min {
        *min = new_min;
    };
    if new_max > *max {
        *max = new_max;
    };
    *sum += new_sum;
    *count += new_count;
}
