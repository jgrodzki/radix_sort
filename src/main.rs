use core::panic;
use lsd::{radix_sort, radix_sort2};
use rand::Rng;
use rayon::slice::ParallelSliceMut;
use rdst::RadixSort;
use rdxsort::RdxSort;
use std::time::{Duration, Instant};

mod lsd;
mod msd;

fn verify_sorted<T>(data: &[T], sorted: &[T])
where
    T: Ord + Clone,
{
    let mut data_copy = data.to_owned();
    data_copy.sort();
    if sorted != data_copy {
        panic!("Not sorted properly!");
    }
}

fn standard_stable(size: u32, runs: u32) {
    println!("Standard stable");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.sort();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn standard_unstable(size: u32, runs: u32) {
    println!("Standard unstable");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.sort_unstable();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn rayon_stable(size: u32, runs: u32) {
    println!("Rayon stable");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.par_sort();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn rayon_unstable(size: u32, runs: u32) {
    println!("Rayon unstable");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.par_sort_unstable();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn rdst_sort(size: u32, runs: u32) {
    println!("Rdst");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.radix_sort_unstable();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn rdxsort_sort(size: u32, runs: u32) {
    println!("Rdxsort");
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            {
                let tick = Instant::now();
                data.rdxsort();
                total_time += tick.elapsed();
            }
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn bench_sort(size: u32, runs: u32, name: &str, sort: fn(&[u32]) -> Vec<u32>) {
    println!("{}", name);
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            let tick = Instant::now();
            sort(&data);
            total_time += tick.elapsed();
        }
        let size = (size + 1) as usize;
        println!(
            "{:size$}\t{:.3e}",
            10usize.pow(e),
            (total_time / runs).as_secs_f64()
        );
    }
}

fn main() {
    standard_unstable(9, 1);
    standard_stable(9, 1);
    rayon_unstable(9, 1);
    rayon_stable(9, 1);
    rdst_sort(9, 1);
    rdxsort_sort(9, 1);
    bench_sort(9, 1, "Radix sort", radix_sort);
    bench_sort(9, 1, "Radix sort 2", radix_sort2);
}
