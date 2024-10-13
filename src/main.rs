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

fn bench_sort(sizes: &[u32], runs: u32, sorts: &[(&str, fn(&mut [u32]))]) {
    let (names, sorts): (Vec<_>, Vec<_>) = sorts.iter().cloned().unzip();
    println!("{}", "Size\t".to_owned() + &names.join("\t"));
    let mut results = vec![0.; sorts.len()];
    for size in sizes {
        let mut data = vec![0u32; *size as usize];
        for (i, sort) in sorts.iter().enumerate() {
            let mut total_time = Duration::ZERO;
            for _ in 0..runs {
                rand::thread_rng().fill(data.as_mut_slice());
                let tick = Instant::now();
                sort(&mut data);
                total_time += tick.elapsed();
            }
            results[i] = (total_time / runs).as_secs_f64();
        }
        print!("{:.1}M", *size as f64 / 1e6);
        for r in &results {
            print!("\t{:.3e}", r);
        }
        println!();
    }
}
fn main() {
    bench_sort(
        (5..9)
            .map(|e| 15 * 10u32.pow(e))
            .collect::<Vec<_>>()
            .as_slice(),
        5,
        &[
            ("Standard unstable", <[u32]>::sort_unstable),
            ("Standard stable", <[u32]>::sort),
            ("Rayon unstable", ParallelSliceMut::par_sort_unstable),
            ("Rayon stable", ParallelSliceMut::par_sort),
            ("Rdst", RadixSort::radix_sort_unstable),
            ("Rdxsort", RdxSort::rdxsort),
            ("Radix", radix_sort),
            ("Radix 2", radix_sort2),
        ],
    );
}
