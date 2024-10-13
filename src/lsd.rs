use core::slice;
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

//10000000 - 68ms
pub fn radix_sort(data: &[u32]) -> Vec<u32> {
    let mut sorted = vec![0; data.len()];
    let mut copy = vec![0; data.len()];
    thread::scope(|s| {
        let thread_0 = s.spawn(|| counting_sort(data, 0));
        let thread_1 = s.spawn(|| counting_sort(data, 1));
        let thread_2 = s.spawn(|| counting_sort(data, 2));
        let thread_3 = s.spawn(|| counting_sort(data, 3));
        let mut o0 = thread_0.join().unwrap();
        for e in data.iter().rev() {
            let i = &mut o0[*e as u8 as usize];
            *i -= 1;
            copy[*i] = *e;
        }
        let mut o1 = thread_1.join().unwrap();
        for e in copy.iter().rev() {
            let i = &mut o1[(*e >> 8) as u8 as usize];
            *i -= 1;
            sorted[*i] = *e;
        }
        let mut o2 = thread_2.join().unwrap();
        for e in sorted.iter().rev() {
            let i = &mut o2[(*e >> 16) as u8 as usize];
            *i -= 1;
            copy[*i] = *e;
        }
        let mut o3 = thread_3.join().unwrap();
        for e in copy.iter().rev() {
            let i = &mut o3[(*e >> 24) as u8 as usize];
            *i -= 1;
            sorted[*i] = *e;
        }
    });
    sorted
}

fn count(data: &[u32], byte: u8) -> Vec<usize> {
    let mut counts = vec![0; 256];
    for n in data {
        counts[(*n >> (byte * 8)) as u8 as usize] += 1;
    }
    counts
}

pub fn radix_sort2(data: &[u32]) -> Vec<u32> {
    let num_cpus = num_cpus::get();
    let cpu_workload = data.len() / num_cpus;
    let mut copy1 = vec![0; data.len()];
    let mut copy2 = vec![0; data.len()];
    let copy1addr = copy1.as_mut_ptr() as usize;
    let copy2addr = copy2.as_mut_ptr() as usize;
    //0 data > copy2
    //1 copy2 > copy1
    //2 copy1 > copy2
    //3 copy2 > copy1
    //TODO: return directly from scope
    // let mut o0 = Vec::new();
    // let mut o1 = Vec::new();
    // let mut o2 = Vec::new();
    // let mut o3 = Vec::new();
    for digit in 0..4 {
        let src = if digit == 0 {
            data
        } else if digit % 2 == 0 {
            copy1.as_slice()
        } else {
            copy2.as_slice()
        };
        let dstaddr = if digit % 2 == 0 { copy2addr } else { copy1addr };
        let mut counts = thread::scope(|s| {
            let workers = (0..num_cpus)
                .map(|c| {
                    let left_bound = c * cpu_workload;
                    let right_bound = if (c + 1) == num_cpus {
                        data.len()
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
            // let thread_0 = s.spawn(|| count(&src[0..2500000], digit));
            // let thread_1 = s.spawn(|| count(&src[2500000..5000000], digit));
            // let thread_2 = s.spawn(|| count(&src[5000000..7500000], digit));
            // let thread_3 = s.spawn(|| count(&src[7500000..10000000], digit));
            // o0 = thread_0.join().unwrap();
            // o1 = thread_1.join().unwrap();
            // o2 = thread_2.join().unwrap();
            // o3 = thread_3.join().unwrap();
        });
        // let mut indexes = (0..256)
        //     .map(|i| o0[i] + o1[i] + o2[i] + o3[i])
        //     .collect::<Vec<_>>();
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
            // o0[i] = indexes[i] - o1[i] - o2[i] - o3[i];
            // o1[i] = indexes[i] - o2[i] - o3[i];
            // o2[i] = indexes[i] - o3[i];
        }
        // for i in 0..256 {
        //     o0[i] = indexes[i] - o1[i] - o2[i] - o3[i];
        //     o1[i] = indexes[i] - o2[i] - o3[i];
        //     o2[i] = indexes[i] - o3[i];
        // }
        // o3 = indexes;
        // thread::scope(|s| {
        //     s.spawn(|| {
        //         for e in src[0..2500000].iter().rev() {
        //             let i = &mut o0[(*e >> (digit * 8)) as u8 as usize];
        //             *i -= 1;
        //             unsafe {
        //                 let s = slice::from_raw_parts_mut(dstaddr as *mut u32, data.len());
        //                 s[*i] = *e;
        //             }
        //         }
        //     });
        //     s.spawn(|| {
        //         for e in src[2500000..5000000].iter().rev() {
        //             let i = &mut o1[(*e >> (digit * 8)) as u8 as usize];
        //             *i -= 1;
        //             unsafe {
        //                 let s = slice::from_raw_parts_mut(dstaddr as *mut u32, data.len());
        //                 s[*i] = *e;
        //             }
        //         }
        //     });
        //     s.spawn(|| {
        //         for e in src[5000000..7500000].iter().rev() {
        //             let i = &mut o2[(*e >> (digit * 8)) as u8 as usize];
        //             *i -= 1;
        //             unsafe {
        //                 let s = slice::from_raw_parts_mut(dstaddr as *mut u32, data.len());
        //                 s[*i] = *e;
        //             }
        //         }
        //     });
        //     s.spawn(|| {
        //         for e in src[7500000..10000000].iter().rev() {
        //             let i = &mut o3[(*e >> (digit * 8)) as u8 as usize];
        //             *i -= 1;
        //             unsafe {
        //                 let s = slice::from_raw_parts_mut(dstaddr as *mut u32, data.len());
        //                 s[*i] = *e;
        //             }
        //         }
        //     });
        // });
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
                            let s = slice::from_raw_parts_mut(dstaddr as *mut u32, data.len());
                            s[*i] = *e;
                        }
                    }
                });
            });
        });
    }
    copy1
}
