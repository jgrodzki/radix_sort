use distr::{KeyUniform, MyExp, StepUniformU32, StepUniformU64, ZipfU32, ZipfU64};
use radix_sort::{RadixDigits, RadixSort};
use rand::{thread_rng, Rng};
use rand_distr::{Distribution, Standard};
use rdst::RadixKey;
use std::{
    fs::File,
    io::{Read, Write},
    mem::transmute,
    path::Path,
    process::Command,
    time::{Duration, Instant},
};

mod distr;
mod radix_sort;

macro_rules! bench_sorts {
    ($size: expr, $runs: expr, $distr: expr, $sorts: expr, [$($type: ty), +]) => {
        let sort_names = stringify!($sorts).trim_matches(|c|c=='['||c==']').split(',').map(str::trim).collect::<Vec<_>>();
        let max_len = sort_names.iter().map(|s| s.len()).max().unwrap();
        println!("SIZE: {}GB RUNS: {}", $size, $runs);
        $(
        {
            println!("\nTYPE: {}", stringify!($type));
            for (t, s) in bench_sorts::<$type, _>(
                $size,
                $runs,
                $distr,
                &$sorts
            ).iter().zip(&sort_names) {
                println!("{:max_len$} {:.4e}s", s, t);
            }
        }
        )+
    };
}

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
    T: RadixDigits + Copy + RadixKey,
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

#[derive(Clone, Copy)]
struct TU32(u32, u32);

impl RadixKey for TU32 {
    const LEVELS: usize = 4;

    fn get_level(&self, level: usize) -> u8 {
        self.0.get_level(level)
    }
}

impl RadixDigits for TU32 {
    const NUMBER_OF_DIGITS: u8 = 4;

    fn get_digit(&self, index: u8) -> u8 {
        self.0.get_digit(index)
    }
}

#[derive(Clone, Copy)]
struct TU64(u64, u64);

impl RadixKey for TU64 {
    const LEVELS: usize = 8;

    fn get_level(&self, level: usize) -> u8 {
        self.0.get_level(level)
    }
}

impl RadixDigits for TU64 {
    const NUMBER_OF_DIGITS: u8 = 8;

    fn get_digit(&self, index: u8) -> u8 {
        self.0.get_digit(index)
    }
}

fn run_tests32(test_name: &str) {
    println!("{}", test_name);
    let n = [
        "RADIX",
        "RDST",
        "UNSTABLE",
        "STABLE",
        "STABLE (rerun)",
        "UNSTABLE (rerun)",
        "RDST (rerun)",
        "RADIX (rerun)",
    ];
    for (t, n) in bench_sorts_data(
        &mut [
            File::open("1.data").unwrap(),
            File::open("2.data").unwrap(),
            File::open("3.data").unwrap(),
            File::open("4.data").unwrap(),
            File::open("5.data").unwrap(),
        ],
        &[
            |d| RadixSort::<TU32>::radix_sort(d, 10),
            rdst::RadixSort::<TU32>::radix_sort_unstable,
            |e: &mut [TU32]| e.sort_unstable_by_key(|e| e.0),
            |e: &mut [TU32]| {
                e.sort_by_key(|e| e.0);
            },
            |e: &mut [TU32]| {
                e.sort_by_key(|e| e.0);
            },
            |e: &mut [TU32]| e.sort_unstable_by_key(|e| e.0),
            rdst::RadixSort::<TU32>::radix_sort_unstable,
            |d| RadixSort::<TU32>::radix_sort(d, 10),
        ],
    )
    .iter()
    .zip(n)
    {
        println!("{}: {:.4e}s", n, t);
    }

    {
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i1.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i2.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i3.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i4.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i5.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }

    {
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i5.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i4.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i3.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i2.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i1.data", "-rec_size8", "-key_size4", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }
}

fn run_tests64(test_name: &str) {
    println!("{}", test_name);
    let n = [
        "RADIX",
        "RDST",
        "UNSTABLE",
        "STABLE",
        "STABLE (rerun)",
        "UNSTABLE (rerun)",
        "RDST (rerun)",
        "RADIX (rerun)",
    ];
    for (t, n) in bench_sorts_data(
        &mut [
            File::open("1.data").unwrap(),
            File::open("2.data").unwrap(),
            File::open("3.data").unwrap(),
            File::open("4.data").unwrap(),
            File::open("5.data").unwrap(),
        ],
        &[
            |d| RadixSort::<TU64>::radix_sort(d, 10),
            rdst::RadixSort::<TU64>::radix_sort_unstable,
            |e: &mut [TU64]| e.sort_unstable_by_key(|e| e.0),
            |e: &mut [TU64]| {
                e.sort_by_key(|e| e.0);
            },
            |e: &mut [TU64]| {
                e.sort_by_key(|e| e.0);
            },
            |e: &mut [TU64]| e.sort_unstable_by_key(|e| e.0),
            rdst::RadixSort::<TU64>::radix_sort_unstable,
            |d| RadixSort::<TU64>::radix_sort(d, 10),
        ],
    )
    .iter()
    .zip(n)
    {
        println!("{}: {:.4e}s", n, t);
    }

    {
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i1.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i2.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i3.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i4.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i5.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }

    {
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i5.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i4.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i3.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i2.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
        let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
            .args(["-i1.data", "-rec_size16", "-key_size8", "-n_threads10"])
            .spawn()
            .unwrap();
        c.wait().unwrap();
    }
}

fn gen_datasets32only<U: Distribution<u32>>(distr: &U) {
    gen_data(4., Path::new("1.data"), &distr);
    gen_data(4., Path::new("2.data"), &distr);
    gen_data(4., Path::new("3.data"), &distr);
    gen_data(4., Path::new("4.data"), &distr);
    gen_data(4., Path::new("5.data"), &distr);
}

fn gen_datasets32<U: Distribution<(u32, u32)>>(distr: &U) {
    gen_data(4., Path::new("1.data"), &distr);
    gen_data(4., Path::new("2.data"), &distr);
    gen_data(4., Path::new("3.data"), &distr);
    gen_data(4., Path::new("4.data"), &distr);
    gen_data(4., Path::new("5.data"), &distr);
}

fn gen_datasets64<U: Distribution<(u64, u64)>>(distr: &U) {
    gen_data(4., Path::new("1.data"), &distr);
    gen_data(4., Path::new("2.data"), &distr);
    gen_data(4., Path::new("3.data"), &distr);
    gen_data(4., Path::new("4.data"), &distr);
    gen_data(4., Path::new("5.data"), &distr);
}

fn main() {
    let sizes = [0.5, 1., 1.5, 2., 2.5, 3., 3.5, 4.];
    for s in sizes {
        bench_sorts!(
            s,
            5,
            &Standard,
            [|e| RadixSort::radix_sort(e, 12)],
            [u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64]
        );
    }
    // let n = [1, 2, 4, 6, 8, 10, 12, 14, 16, 16, 14, 12, 10, 8, 6, 4, 2, 1];
    // for (t, n) in bench_sorts(
    //     4.,
    //     5,
    //     &Standard,
    //     &[
    //         |d| <[u32]>::radix_sort(d, 1),
    //         |d| <[u32]>::radix_sort(d, 2),
    //         |d| <[u32]>::radix_sort(d, 4),
    //         |d| <[u32]>::radix_sort(d, 6),
    //         |d| <[u32]>::radix_sort(d, 8),
    //         |d| <[u32]>::radix_sort(d, 10),
    //         |d| <[u32]>::radix_sort(d, 12),
    //         |d| <[u32]>::radix_sort(d, 14),
    //         |d| <[u32]>::radix_sort(d, 16),
    //         |d| <[u32]>::radix_sort(d, 16),
    //         |d| <[u32]>::radix_sort(d, 14),
    //         |d| <[u32]>::radix_sort(d, 12),
    //         |d| <[u32]>::radix_sort(d, 10),
    //         |d| <[u32]>::radix_sort(d, 8),
    //         |d| <[u32]>::radix_sort(d, 6),
    //         |d| <[u32]>::radix_sort(d, 4),
    //         |d| <[u32]>::radix_sort(d, 2),
    //         |d| <[u32]>::radix_sort(d, 1),
    //     ],
    // )
    // .iter()
    // .zip(n)
    // {
    //     println!("{}: {:.4e}s", n, t);
    // }
    // {
    //     let distr = KeyUniform;
    //     gen_datasets32(&distr);
    //     let n = [1, 2, 4, 6, 8, 10, 12, 14, 16, 16, 14, 12, 10, 8, 6, 4, 2, 1];
    //     for (t, n) in bench_sorts_data(
    //         &mut [
    //             File::open("1.data").unwrap(),
    //             File::open("2.data").unwrap(),
    //             File::open("3.data").unwrap(),
    //             File::open("4.data").unwrap(),
    //             File::open("5.data").unwrap(),
    //         ],
    //         &[
    //             |d| RadixSort::<TU32>::radix_sort(d, 1),
    //             |d| RadixSort::<TU32>::radix_sort(d, 2),
    //             |d| RadixSort::<TU32>::radix_sort(d, 4),
    //             |d| RadixSort::<TU32>::radix_sort(d, 6),
    //             |d| RadixSort::<TU32>::radix_sort(d, 8),
    //             |d| RadixSort::<TU32>::radix_sort(d, 10),
    //             |d| RadixSort::<TU32>::radix_sort(d, 12),
    //             |d| RadixSort::<TU32>::radix_sort(d, 14),
    //             |d| RadixSort::<TU32>::radix_sort(d, 16),
    //             |d| RadixSort::<TU32>::radix_sort(d, 16),
    //             |d| RadixSort::<TU32>::radix_sort(d, 14),
    //             |d| RadixSort::<TU32>::radix_sort(d, 12),
    //             |d| RadixSort::<TU32>::radix_sort(d, 10),
    //             |d| RadixSort::<TU32>::radix_sort(d, 8),
    //             |d| RadixSort::<TU32>::radix_sort(d, 6),
    //             |d| RadixSort::<TU32>::radix_sort(d, 4),
    //             |d| RadixSort::<TU32>::radix_sort(d, 2),
    //             |d| RadixSort::<TU32>::radix_sort(d, 1),
    //         ],
    //     )
    //     .iter()
    //     .zip(n)
    //     {
    //         println!("{}: {:.4e}s", n, t);
    //     }
    //     let t = [1, 2, 4, 6, 8, 10, 12, 14, 16];
    //     for t in t {
    //         println!(
    //             "----------------------------{}-----------------------------",
    //             t
    //         );
    //         for f in 1..=5 {
    //             let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
    //                 .args([
    //                     &("-i".to_owned() + &f.to_string() + ".data"),
    //                     "-rec_size8",
    //                     "-key_size4",
    //                     &("-n_threads".to_owned() + &t.to_string()),
    //                 ])
    //                 .spawn()
    //                 .unwrap();
    //             c.wait().unwrap();
    //         }
    //         for f in (1..=5).rev() {
    //             let mut c = Command::new("/home/kuba/RADULS/bin/raduls")
    //                 .args([
    //                     &("-i".to_owned() + &f.to_string() + ".data"),
    //                     "-rec_size8",
    //                     "-key_size4",
    //                     &("-n_threads".to_owned() + &t.to_string()),
    //                 ])
    //                 .spawn()
    //                 .unwrap();
    //             c.wait().unwrap();
    //         }
    //     }
    // }
}
