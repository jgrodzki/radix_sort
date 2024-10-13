use core::slice::{self};
use std::thread;

fn counting_sort(data: &[u32], byte: u8) -> Vec<usize> {
    let mut counts = vec![0usize; 256];
    for n in data {
        counts[(*n >> (byte * 8)) as u8 as usize] += 1;
    }

    counts.iter_mut().reduce(|acc, e| {
        *e += *acc;
        e
    });
    counts
}

pub fn radix_sort(data: &mut [u32]) {
    let mut copy = vec![0; data.len()];
    let mut counts = thread::scope(|s| {
        let data_b = &data;
        let workers = (0..4)
            .map(|b| s.spawn(move || counting_sort(data_b, b)))
            .collect::<Vec<_>>();
        workers
            .into_iter()
            .map(|h| h.join().unwrap())
            .collect::<Vec<_>>()
    });
    for (b, c) in counts.iter_mut().enumerate() {
        let (src, dst) = if b % 2 == 0 {
            (&mut *data, copy.as_mut_slice())
        } else {
            (copy.as_mut_slice(), &mut *data)
        };
        for e in src.iter().rev() {
            let i = &mut c[(*e >> b * 8) as u8 as usize];
            *i -= 1;
            dst[*i] = *e;
        }
    }
}

fn count(data: &[u32], byte: u8) -> Vec<usize> {
    let mut counts = vec![0; 256];
    for n in data {
        counts[(*n >> (byte * 8)) as u8 as usize] += 1;
    }
    counts
}

pub fn radix_sort2(data: &mut [u32]) {
    let num_cpus = num_cpus::get();
    let data_len = data.len();
    let cpu_workload = data_len / num_cpus;
    let copy = vec![0; data_len];
    for digit in 0..4 {
        let (src, dst) = if digit % 2 == 0 {
            (&*data, copy.as_slice())
        } else {
            (copy.as_slice(), &*data)
        };
        let mut counts = thread::scope(|s| {
            let workers = (0..num_cpus)
                .map(|c| {
                    let left_bound = c * cpu_workload;
                    let right_bound = if (c + 1) == num_cpus {
                        data_len
                    } else {
                        (c + 1) * cpu_workload
                    };
                    s.spawn(move || count(&src[left_bound..right_bound], digit))
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        let mut indexes = (0..256)
            .map(|i| counts.iter().map(|c| c[i]).sum())
            .collect::<Vec<usize>>();
        indexes.iter_mut().reduce(|acc, e| {
            *e += *acc;
            e
        });
        for i in 0..256 {
            for c in 0..num_cpus {
                let n = counts[c + 1..num_cpus].iter().map(|e| e[i]).sum::<usize>();
                counts[c][i] = indexes[i] - n;
            }
        }
        thread::scope(|s| {
            (0..num_cpus).rev().for_each(|c| {
                let left_bound = c * cpu_workload;
                let right_bound = if (c + 1) == num_cpus {
                    data.len()
                } else {
                    (c + 1) * cpu_workload
                };
                let mut counts = counts.pop().unwrap();
                s.spawn(move || {
                    for e in src[left_bound..right_bound].iter().rev() {
                        let i = &mut counts[(*e >> (digit * 8)) as u8 as usize];
                        *i -= 1;
                        unsafe {
                            let s = slice::from_raw_parts_mut(dst.as_ptr() as *mut u32, data_len);
                            s[*i] = *e;
                        }
                    }
                });
            });
        });
    }
}
