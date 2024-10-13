use core::panic;
use lsd::{radix_sort, radix_sort2};
use rand::Rng;
use rayon::slice::ParallelSliceMut;
use rdst::RadixSort;
use rdxsort::RdxSort;
use std::time::{Duration, Instant};

mod lsd;
mod msd;

fn verify_sorted<T>(data: &[T])
where
    T: Ord + Clone,
{
    let mut data_copy = data.to_owned();
    data_copy.sort();
    if data != data_copy {
        panic!("Not sorted properly!");
    }
}

fn bench_sort(size: u32, runs: u32, name: &str, sort: fn(&mut [u32])) {
    println!("{}", name);
    for e in 1..=size {
        let mut total_time = Duration::ZERO;
        let mut data = vec![0u32; 10usize.pow(e)];
        for _ in 0..runs {
            rand::thread_rng().fill(data.as_mut_slice());
            let tick = Instant::now();
            sort(&mut data);
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
    bench_sort(9, 5, "Standard unstable", <[u32]>::sort_unstable);
    bench_sort(9, 5, "Standard stable", <[u32]>::sort);
    bench_sort(9, 5, "Rayon unstable", ParallelSliceMut::par_sort_unstable);
    bench_sort(9, 5, "Rayon stable", ParallelSliceMut::par_sort);
    bench_sort(9, 5, "Rdst", RadixSort::radix_sort_unstable);
    bench_sort(9, 5, "Rdxsort", RdxSort::rdxsort);
    bench_sort(9, 5, "Radix", radix_sort);
    bench_sort(9, 5, "Radix 2", radix_sort2);
}
