use arbitrary_chunks::ArbitraryChunks;
use core::slice;
pub use radix_digit::RadixDigit;
use rayon::{
    current_num_threads,
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSlice,
};
use std::thread;

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
        let num_cpus = num_cpus::get();
        let data_len = self.len();
        let cpu_workload = data_len / num_cpus;
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
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
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
                    chunk
                        .iter()
                        .zip(&mut ends)
                        .for_each(|(bin, end)| *end = bin.len());
                    for e in c.iter().rev() {
                        let d = e.get_digit(digit) as usize;
                        ends[d] -= 1;
                        chunk[d][ends[d]] = *e;
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            //TODO: make parallel
            self.swap_with_slice(copy.as_mut_slice());
        }
    }
}
