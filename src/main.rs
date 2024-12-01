use radix_sort::{RadixDigit, RadixSort};
use rand::{Fill, Rng};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

mod msd;
mod radix_sort;
mod rdst_lsb;

fn verify_sorted<T>(data: &[T], original: Option<&mut [T]>)
where
    T: Clone + Ord + Debug,
{
    if let Some(original) = original {
        original.sort();
        if data != original {
            panic!("Not sorted properly!");
        }
    } else {
        let mut copy = data.to_owned();
        copy.sort();
        if data != copy {
            panic!("Not sorted properly!");
        }
    }
}

fn bench_sort<T>(sizes: &[u32], runs: u32, sorts: &[(&str, fn(&mut [T]))])
where
    T: RadixDigit + Default + Copy + Ord,
    [T]: Fill,
{
    let (names, sorts): (Vec<_>, Vec<_>) = sorts.iter().cloned().unzip();
    println!("{}", "Size\t".to_owned() + &names.join("\t"));
    let mut results = vec![0.; sorts.len()];
    for size in sizes {
        let mut data = vec![T::default(); *size as usize];
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
    bench_sort::<u32>(
        &[500000000, 1000000000, 1500000000],
        // &[1000000000],
        5,
        &[
            // ("Standard unstable", <[u8]>::sort_unstable),
            // ("Standard stable", <[u8]>::sort),
            // ("Rayon unstable", ParallelSliceMut::par_sort_unstable),
            // ("Rayon stable", ParallelSliceMut::par_sort),
            // ("Rdst", rdst::RadixSort::radix_sort_unstable),
            // ("Radix", RadixSort::radix_sort),
            // ("Radix 2", RadixSort::radix_sort2),
            // ("Radix 3", RadixSort::radix_sort3),
            // ("Radix 4", RadixSort::radix_sort4),
            // ("Radix 5", RadixSort::radix_sort5),
            ("Radix 6", RadixSort::radix_sort6),
            ("Radix 7", RadixSort::radix_sort7),
            ("Radix 8", RadixSort::radix_sort8),
            // ("LSB", |e| {
            //     let cpu_workload = {
            //         let num_cpus: usize = available_parallelism().unwrap().into();
            //         (500000000 + num_cpus - 1) / num_cpus
            //     };
            //     rdst_lsb::mt_lsb_sort_adapter(e, 0, 3, cpu_workload);
            // }),
        ],
    );
    // let mut data = vec![0u32; 1000000];
    // rand::thread_rng().fill(data.as_mut_slice());
    // let mut copy = data.clone();
    // data.radix_sort8();
    // verify_sorted(&data, Some(&mut copy));
}
