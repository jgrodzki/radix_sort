use radix_sort::{RadixDigit, RadixSortNonCopy};
use rand::{Fill, Rng};
use std::{
    fmt::Debug,
    time::{Duration, Instant},
};

mod radix_sort;

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

fn bench_sort<T>(sizes: &[usize], runs: u32, sorts: &[(&str, fn(&mut [T]))])
where
    T: RadixDigit + Default + Copy,
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

fn bench_type<T>(size: f32, runs: u32) -> f64
where
    T: RadixDigit + Default + Clone,
    [T]: Fill,
{
    let size = (size * 1e9 / size_of::<T>() as f32) as usize;
    let mut data = vec![T::default(); size];
    let mut total_time = Duration::ZERO;
    for _ in 0..runs {
        rand::thread_rng().fill(data.as_mut_slice());
        let tick = Instant::now();
        data.radix_sort7();
        total_time += tick.elapsed();
    }
    (total_time / runs).as_secs_f64()
}

fn bench_types(sizes: &[f32], runs: u32) {
    println!("Radix 7\nSize\tu8\ti8\tu16\ti16\tu32\ti32\tu64\ti64\tu128\ti128\tf32\tf64");
    for size in sizes {
        print!("{:.1}G", size);
        print!("\t{:.3e}", bench_type::<u8>(*size, runs));
        print!("\t{:.3e}", bench_type::<i8>(*size, runs));
        print!("\t{:.3e}", bench_type::<u16>(*size, runs));
        print!("\t{:.3e}", bench_type::<i16>(*size, runs));
        print!("\t{:.3e}", bench_type::<u32>(*size, runs));
        print!("\t{:.3e}", bench_type::<i32>(*size, runs));
        print!("\t{:.3e}", bench_type::<u64>(*size, runs));
        print!("\t{:.3e}", bench_type::<i64>(*size, runs));
        print!("\t{:.3e}", bench_type::<u128>(*size, runs));
        print!("\t{:.3e}", bench_type::<i128>(*size, runs));
        print!("\t{:.3e}", bench_type::<f32>(*size, runs));
        print!("\t{:.3e}", bench_type::<f64>(*size, runs));
        println!();
    }
}

#[derive(Debug)]
struct TestType {
    key: u32,
    value: String,
}

impl Drop for TestType {
    fn drop(&mut self) {
        println!("Dropping!");
    }
}

impl RadixDigit for TestType {
    const DIGITS: u8 = 4;

    fn get_digit(&self, digit: u8) -> u8 {
        self.key.get_digit(digit)
    }
}

fn main() {
    bench_types(&[2., 4., 6.], 5);
    // let mut data = [
    //     TestType {
    //         key: u32::MAX - 1,
    //         value: String::from_str("D").unwrap(),
    //     },
    //     TestType {
    //         key: u32::MAX - 50,
    //         value: String::from_str("C").unwrap(),
    //     },
    //     TestType {
    //         key: u32::MAX - 11234,
    //         value: String::from_str("A").unwrap(),
    //     },
    //     TestType {
    //         key: u32::MAX - 11234,
    //         value: String::from_str("B").unwrap(),
    //     },
    // ];
    // rand::thread_rng().fill(data.as_mut_slice());
    // println!("{:?}", data);
    // data.radix_sort7();
    // println!("Finished sorting!");
    // println!("{:?}", data);
    // let mut copy = data.clone();
    // verify_sorted(&data, Some(&mut copy));
}
