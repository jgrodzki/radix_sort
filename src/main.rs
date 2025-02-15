use distr::KeyUniform;
use radix_sort::{RadixDigits, RadixSort};
use rand::{thread_rng, Rng};
use rand_distr::Distribution;
use std::{
    fs::File,
    io::{Read, Write},
    mem::transmute,
    path::Path,
    time::{Duration, Instant},
};

mod distr;
mod radix_sort;

fn bench_sorts<T, U>(size: f32, runs: u32, distr: &U, sorts: &[fn(&mut [T])]) -> Vec<f64>
where
    T: RadixDigits + Copy,
    U: Distribution<T>,
{
    if sorts.len() == 0 || runs == 0 {
        return vec![];
    }
    let size = (size * 1e9 / size_of::<T>() as f32) as usize;
    let mut results = vec![Duration::ZERO; sorts.len()];
    for _ in 0..runs {
        let mut data = thread_rng()
            .sample_iter(distr)
            .take(size)
            .collect::<Vec<T>>();
        if sorts.len() == 1 {
            let tick = Instant::now();
            sorts[0](&mut data);
            results[0] += tick.elapsed()
        } else {
            for (sort, time) in sorts.iter().zip(&mut results) {
                let mut clone = data.clone();
                let tick = Instant::now();
                sort(&mut clone);
                *time += tick.elapsed()
            }
        }
    }
    results.iter().map(|t| (*t / runs).as_secs_f64()).collect()
}

fn bench_sorts_data<T>(files: &mut [File], sorts: &[fn(&mut [T])]) -> Vec<f64>
where
    T: RadixDigits + Copy,
{
    if sorts.len() == 0 || files.len() == 0 {
        return vec![];
    }
    let mut results = vec![Duration::ZERO; sorts.len()];
    for file in files.iter_mut() {
        let mut data: Vec<T> = {
            let mut data = Vec::new();
            file.read_to_end(&mut data).unwrap();
            unsafe {
                data.set_len(data.len() / size_of::<T>());
                transmute(data)
            }
        };
        if sorts.len() == 1 {
            let tick = Instant::now();
            sorts[0](&mut data);
            results[0] += tick.elapsed()
        } else {
            for (sort, time) in sorts.iter().zip(&mut results) {
                let mut clone = data.clone();
                let tick = Instant::now();
                sort(&mut clone);
                *time += tick.elapsed()
            }
        }
    }
    results
        .iter()
        .map(|t| (*t / files.len() as u32).as_secs_f64())
        .collect()
}

fn gen_data<T, U>(size: f32, file: &Path, distr: &U)
where
    T: RadixDigits,
    U: Distribution<T>,
{
    let size = (size * 1e9 / size_of::<T>() as f32) as usize;
    let mut data = thread_rng()
        .sample_iter(distr)
        .take(size)
        .collect::<Vec<T>>();
    let data: Vec<u8> = unsafe {
        data.set_len(data.len() * size_of::<T>());
        transmute(data)
    };
    let mut file = File::create(file).unwrap();
    file.write_all(&data).unwrap();
}

fn main() {
    let number_of_runs = 5;
    let distribution = KeyUniform;
    let sizes = [0.5, 1., 1.5, 2., 2.5, 3., 3.5, 4.];

    println!("\nTYPE: u32/u32 RUNS: {}", number_of_runs);
    for size in sizes {
        let results = bench_sorts(
            size,
            number_of_runs,
            &distribution,
            &[<[(u32, u32)]>::radix_sort],
        );
        println!("Sorted {:.1}GB of data in: {:.4}s", size, results[0]);
    }

    println!("\nTYPE: u64/u64 RUNS: {}", number_of_runs);
    for size in sizes {
        let results = bench_sorts(
            size,
            number_of_runs,
            &distribution,
            &[<[(u64, u64)]>::radix_sort],
        );
        println!("Sorted {:.1}GB of data in: {:.4}s", size, results[0]);
    }
}
