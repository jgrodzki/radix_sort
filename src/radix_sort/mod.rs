use core::slice;
pub use radix_digit::RadixDigit;
use std::thread;

mod radix_digit;
#[cfg(test)]
mod tests;

pub trait RadixSort {
    fn radix_sort(&mut self);
    fn radix_sort2(&mut self);
}

fn count(data: &[impl RadixDigit], digit: u8) -> Vec<usize> {
    let mut counts = vec![0; 256];
    for n in data {
        counts[n.get_digit(digit) as usize] += 1;
    }
    counts
}

fn count_and_cumsum(data: &[impl RadixDigit], digit: u8) -> Vec<usize> {
    let mut counts = vec![0usize; 256];
    for n in data {
        counts[n.get_digit(digit) as usize] += 1;
    }
    counts.iter_mut().reduce(|acc, e| {
        *e += *acc;
        e
    });
    counts
}

impl<T: RadixDigit> RadixSort for [T] {
    fn radix_sort(&mut self) {
        let mut copy = self.to_owned();
        let mut counts = thread::scope(|s| {
            let data_b = &self;
            let workers = (0..T::DIGITS)
                .map(|d| s.spawn(move || count_and_cumsum(data_b, d)))
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
        //TODO: get rid of this swap, either:
        // - stay with initial clone and swap src/dst for uneven digits
        // - separate implemenation for u8/i8 (realistically the only uneven digit type)
        // - keep the swap, but use MaybeUninit for copy, reducing swap count by 1, thus making it on par with previous implementation (+this may be the way to get rid of Clone/Default type reqirements)
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    fn radix_sort2(&mut self) {
        let num_cpus = num_cpus::get();
        let data_len = self.len();
        let cpu_workload = data_len / num_cpus;
        let mut copy = self.to_owned();
        for digit in 0..T::DIGITS {
            let (src, dst) = if digit % 2 == 0 {
                (&*self, copy.as_slice())
            } else {
                (copy.as_slice(), &*self)
            };
            let mut counts = thread::scope(|s| {
                //TODO: optimize for small counts?
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
}
