use arbitrary_chunks::ArbitraryChunks;
use core::slice;
pub use radix_digit::RadixDigit;
use rayon::{
    current_num_threads,
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSlice,
};
use std::{
    thread::{self, available_parallelism},
    time::{Duration, Instant},
};

mod radix_digit;
#[cfg(test)]
mod tests;

pub trait RadixSort {
    fn radix_sort(&mut self);
    fn radix_sort2(&mut self);
    fn radix_sort3(&mut self);
    fn radix_sort4(&mut self);
}

impl<T: RadixDigit> RadixSort for [T] {
    fn radix_sort(&mut self) {
        let mut copy = Vec::with_capacity(self.len());
        unsafe {
            copy.set_len(self.len());
        }
        let mut counts = thread::scope(|s| {
            let data_b = &self;
            let workers = (0..T::DIGITS)
                .map(|digit| {
                    s.spawn(move || {
                        let mut counts = vec![0usize; 256];
                        for n in data_b.as_ref() {
                            counts[n.get_digit(digit) as usize] += 1;
                        }
                        counts.iter_mut().reduce(|acc, e| {
                            *e += *acc;
                            e
                        });
                        counts
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        for (d, c) in counts.iter_mut().enumerate() {
            let (src, dst) = if d % 2 == 0 {
                (&mut *self, copy.as_mut_slice())
            } else {
                (copy.as_mut_slice(), &mut *self)
            };
            for e in src.iter().rev() {
                let i = &mut c[e.get_digit(d as u8) as usize];
                *i -= 1;
                dst[*i] = *e;
            }
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    fn radix_sort2(&mut self) {
        let num_cpus = available_parallelism().unwrap().get() * 4;
        let data_len = self.len();
        let cpu_workload = data_len / num_cpus;
        let mut copy = Vec::with_capacity(self.len());
        unsafe {
            copy.set_len(self.len());
        }
        for digit in 0..T::DIGITS {
            let (src, dst) = if digit % 2 == 0 {
                (&*self, &*copy)
            } else {
                (&*copy, &*self)
            };
            let mut t1 = Duration::ZERO;
            let st1 = Instant::now();
            let mut counts = thread::scope(|s| {
                let workers = (0..num_cpus)
                    .map(|c| {
                        let left_bound = c * cpu_workload;
                        let right_bound = if (c + 1) == num_cpus {
                            data_len
                        } else {
                            (c + 1) * cpu_workload
                        };
                        s.spawn(move || {
                            let mut counts = [0; 256];
                            for n in &src[left_bound..right_bound] {
                                counts[n.get_digit(digit) as usize] += 1;
                            }
                            counts
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|h| h.join().unwrap())
                    .collect::<Vec<_>>()
            });
            t1 += st1.elapsed();
            println!("COUNT: {:.3e}", t1.as_secs_f64());
            let mut t2 = Duration::ZERO;
            let st2 = Instant::now();
            let mut sum = 0;
            for e in 0..256 {
                for a in 0..num_cpus {
                    sum += counts[a][e];
                    counts[a][e] = sum;
                }
            }
            t2 += st2.elapsed();
            println!("ACC: {:.3e}", t2.as_secs_f64());
            let mut t3 = Duration::ZERO;
            let st3 = Instant::now();
            thread::scope(|s| {
                (0..num_cpus).rev().for_each(|c| {
                    let left_bound = c * cpu_workload;
                    let right_bound = if (c + 1) == num_cpus {
                        self.len()
                    } else {
                        (c + 1) * cpu_workload
                    };
                    let mut counts = counts.pop().unwrap();
                    s.spawn(move || {
                        for e in src[left_bound..right_bound].iter().rev() {
                            let i = &mut counts[e.get_digit(digit) as usize];
                            *i -= 1;
                            unsafe {
                                let s = slice::from_raw_parts_mut(dst.as_ptr() as *mut T, data_len);
                                s[*i] = *e;
                            }
                        }
                    });
                });
            });
            t3 += st3.elapsed();
            println!("PERMUT: {:.3e}", t3.as_secs_f64());
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(&mut copy);
        }
    }

    fn radix_sort3(&mut self) {
        let cpu_workload = {
            let num_cpus = current_num_threads();
            (self.len() + num_cpus - 1) / num_cpus
        };
        let mut copy = Vec::with_capacity(self.len());
        unsafe {
            copy.set_len(self.len());
        }
        for digit in 0..T::DIGITS {
            let (src, dst) = if digit % 2 == 0 {
                (&*self, copy.as_slice())
            } else {
                (copy.as_slice(), &*self)
            };
            let mut counts = src
                .par_chunks(cpu_workload)
                .map(|e| {
                    let mut counts = [0; 256];
                    for n in e {
                        counts[n.get_digit(digit) as usize] += 1;
                    }
                    counts
                })
                .collect::<Vec<_>>();
            let mut indexes = (0..256)
                .map(|i| counts.iter().map(|c| c[i]).sum())
                .collect::<Vec<usize>>();
            indexes.iter_mut().reduce(|acc, e| {
                *e += *acc;
                e
            });
            for i in 0..256 {
                for c in 0..counts.len() {
                    let n = counts[c + 1..counts.len()]
                        .iter()
                        .map(|e| e[i])
                        .sum::<usize>();
                    counts[c][i] = indexes[i] - n;
                }
            }
            src.par_chunks(cpu_workload)
                .zip(counts)
                .for_each(|(c, mut counts)| {
                    for e in c.iter().rev() {
                        let i = &mut counts[e.get_digit(digit) as usize];
                        *i -= 1;
                        unsafe {
                            let s = slice::from_raw_parts_mut(dst.as_ptr() as *mut T, self.len());
                            s[*i] = *e;
                        }
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    fn radix_sort4(&mut self) {
        let cpu_workload = {
            let num_cpus = current_num_threads();
            (self.len() + num_cpus - 1) / num_cpus
        };
        let mut copy = Vec::with_capacity(self.len());
        unsafe {
            copy.set_len(self.len());
        }
        for digit in 0..T::DIGITS {
            let (src, dst) = if digit % 2 == 0 {
                (&*self, copy.as_mut_slice())
            } else {
                (copy.as_slice(), &mut *self)
            };
            let counts = src
                .par_chunks(cpu_workload)
                .map(|e| {
                    let mut counts = [0; 256];
                    for n in e {
                        counts[n.get_digit(digit) as usize] += 1;
                    }
                    counts
                })
                .collect::<Vec<_>>();
            let mut sorted_counts = Vec::with_capacity(256 * counts.len());
            for b in 0..256 {
                for c in &counts {
                    sorted_counts.push(c[b]);
                }
            }
            let mut dst_chunks = dst.arbitrary_chunks_mut(&sorted_counts).collect::<Vec<_>>();
            dst_chunks.reverse();
            let mut chunks: Vec<Vec<&mut [T]>> = Vec::with_capacity(counts.len());
            chunks.resize_with(counts.len(), || Vec::with_capacity(256));
            for _ in 0..256 {
                for chunk in chunks.iter_mut() {
                    chunk.push(dst_chunks.pop().unwrap());
                }
            }
            src.par_chunks(cpu_workload)
                .zip(chunks)
                .for_each(|(c, mut chunk)| {
                    let mut ends = [0usize; 256];
                    let mut offsets = [0usize; 256];
                    chunk.iter().zip(&mut ends).for_each(|(bin, end)| {
                        if !bin.is_empty() {
                            *end = bin.len() - 1
                        }
                    });

                    let mut left = 0;
                    let mut right = c.len() - 1;
                    let pre = c.len() % 8;

                    for _ in 0..pre {
                        let b = c[right].get_digit(digit) as usize;

                        chunk[b][ends[b]] = c[right];
                        ends[b] = ends[b].wrapping_sub(1);
                        right = right.saturating_sub(1);
                    }

                    if pre == c.len() {
                        return;
                    }

                    let end = (c.len() - pre) / 2;

                    while left < end {
                        let bl_0 = c[left].get_digit(digit) as usize;
                        let bl_1 = c[left + 1].get_digit(digit) as usize;
                        let bl_2 = c[left + 2].get_digit(digit) as usize;
                        let bl_3 = c[left + 3].get_digit(digit) as usize;
                        let br_0 = c[right].get_digit(digit) as usize;
                        let br_1 = c[right - 1].get_digit(digit) as usize;
                        let br_2 = c[right - 2].get_digit(digit) as usize;
                        let br_3 = c[right - 3].get_digit(digit) as usize;

                        chunk[bl_0][offsets[bl_0]] = c[left];
                        offsets[bl_0] += 1;
                        chunk[br_0][ends[br_0]] = c[right];
                        ends[br_0] = ends[br_0].wrapping_sub(1);
                        chunk[bl_1][offsets[bl_1]] = c[left + 1];
                        offsets[bl_1] += 1;
                        chunk[br_1][ends[br_1]] = c[right - 1];
                        ends[br_1] = ends[br_1].wrapping_sub(1);
                        chunk[bl_2][offsets[bl_2]] = c[left + 2];
                        offsets[bl_2] += 1;
                        chunk[br_2][ends[br_2]] = c[right - 2];
                        ends[br_2] = ends[br_2].wrapping_sub(1);
                        chunk[bl_3][offsets[bl_3]] = c[left + 3];
                        offsets[bl_3] += 1;
                        chunk[br_3][ends[br_3]] = c[right - 3];
                        ends[br_3] = ends[br_3].wrapping_sub(1);

                        left += 4;
                        right = right.wrapping_sub(4);
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            //TODO: make parallel
            self.swap_with_slice(copy.as_mut_slice());
        }
    }
}
