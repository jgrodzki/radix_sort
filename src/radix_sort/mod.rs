use libc::{sysconf, _SC_PAGESIZE};
pub use radix_digit::RadixDigit;
use rayon::{
    current_num_threads,
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSlice,
};
use std::{
    array::from_fn,
    mem::{transmute, MaybeUninit},
    ptr::copy_nonoverlapping,
    slice::{self},
    thread::{self, available_parallelism},
};

mod radix_digit;
#[cfg(test)]
mod tests;

pub trait RadixSort {
    fn radix_sort(&mut self);
    fn radix_sort2(&mut self);
    fn radix_sort3(&mut self);
    fn radix_sort4(&mut self);
    fn radix_sort5(&mut self);
    fn radix_sort6(&mut self);
}

pub trait RadixSortNonCopy {
    fn radix_sort7(&mut self);
}

impl<T: RadixDigit> RadixSortNonCopy for [T] {
    //Plain threads + derand + no copy
    fn radix_sort7(&mut self) {
        const BUFFER_SIZE: usize = 96;
        // const PAGE_SIZE: usize = 4096;
        let elements_per_cpu = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let mut temp: Vec<MaybeUninit<T>> = Vec::with_capacity(self.len());
        unsafe {
            // NOT PORTABLE
            let page_size = sysconf(_SC_PAGESIZE) as usize;
            temp.set_len(self.len());
            let as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            as_bytes
                .iter_mut()
                .step_by(page_size)
                .for_each(|element| *element = 0);
        }
        let temp_slice = unsafe { transmute::<&[MaybeUninit<T>], &[T]>(&temp) };
        for current_digit_index in 0..T::DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (&*self, temp_slice)
            } else {
                (temp_slice, &*self)
            };
            let mut digit_histograms_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_cpu)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut digit_histogram = [0; 256];
                            for element in src_chunk {
                                digit_histogram[element.get_digit(current_digit_index) as usize] +=
                                    1;
                            }
                            digit_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle
                            .join()
                            .expect("failed to join count worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let digit_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for chunk_index in 0..digit_histograms_per_chunk.len() {
                        let old_prefix_sum = prefix_sum;
                        prefix_sum += digit_histograms_per_chunk[chunk_index][digit];
                        digit_histograms_per_chunk[chunk_index][digit] = old_prefix_sum;
                    }
                }
                digit_histograms_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_cpu)
                    .zip(digit_starts_per_chunk)
                    .for_each(|(src_chunk, mut digit_starts)| {
                        scope.spawn(move || {
                            let mut derand_buffers: MaybeUninit<[[T; BUFFER_SIZE]; 256]> =
                                MaybeUninit::uninit();
                            let derand_buffers_slice = unsafe { derand_buffers.assume_init_mut() };
                            let mut derand_buffer_sizes = [0; 256];
                            for element in src_chunk {
                                let digit = element.get_digit(current_digit_index) as usize;
                                unsafe {
                                    copy_nonoverlapping(
                                        element,
                                        &derand_buffers_slice[digit][derand_buffer_sizes[digit]]
                                            as *const T
                                            as *mut T,
                                        1,
                                    );
                                }
                                derand_buffer_sizes[digit] += 1;
                                if derand_buffer_sizes[digit] == BUFFER_SIZE {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit].as_ptr() as *const T,
                                            &dst[digit_starts[digit]] as *const T as *mut T,
                                            BUFFER_SIZE,
                                        );
                                    }
                                    digit_starts[digit] += BUFFER_SIZE;
                                    derand_buffer_sizes[digit] = 0;
                                }
                            }
                            for digit in 0..256 {
                                if derand_buffer_sizes[digit] > 0 {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit].as_ptr() as *const T,
                                            &dst[digit_starts[digit]] as *const T as *mut T,
                                            derand_buffer_sizes[digit],
                                        );
                                    }
                                }
                            }
                        });
                    });
            });
        }
        if T::DIGITS % 2 == 1 {
            unsafe {
                copy_nonoverlapping(temp_slice.as_ptr(), self.as_mut_ptr(), self.len());
            }
        }
    }
}

impl<T: RadixDigit + Copy> RadixSort for [T] {
    //Single thread
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

    //Plain threads
    fn radix_sort2(&mut self) {
        let num_cpus = available_parallelism().unwrap().get();
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
            // let mut t1 = Duration::ZERO;
            // let st1 = Instant::now();
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
            // t1 += st1.elapsed();
            // println!("COUNT: {:.3e}", t1.as_secs_f64());
            // let mut t2 = Duration::ZERO;
            // let st2 = Instant::now();
            let mut sum = 0;
            for e in 0..256 {
                for a in 0..num_cpus {
                    sum += counts[a][e];
                    counts[a][e] = sum;
                }
            }
            // t2 += st2.elapsed();
            // println!("ACC: {:.3e}", t2.as_secs_f64());
            // let mut t3 = Duration::ZERO;
            // let st3 = Instant::now();
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
            // t3 += st3.elapsed();
            // println!("PERMUT: {:.3e}", t3.as_secs_f64());
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(&mut copy);
        }
    }

    //Rayon
    fn radix_sort3(&mut self) {
        let cpu_workload = {
            let num_cpus = current_num_threads() * 4;
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
            let mut sum = 0;
            for e in 0..256 {
                for a in 0..counts.len() {
                    let old_sum = sum;
                    sum += counts[a][e];
                    counts[a][e] = old_sum;
                }
            }
            src.par_chunks(cpu_workload)
                .zip(counts)
                .for_each(|(c, mut counts)| {
                    for e in c {
                        let i = &mut counts[e.get_digit(digit) as usize];
                        unsafe {
                            let s = slice::from_raw_parts_mut(dst.as_ptr() as *mut T, self.len());
                            s[*i] = *e;
                        }
                        *i += 1;
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    //Rayon + derand (buffer starts)
    fn radix_sort4(&mut self) {
        const BUFFER_SIZE: usize = 96;
        let num_cpus = current_num_threads() * 4;
        let cpu_workload = (self.len() + num_cpus - 1) / num_cpus;
        let mut copy = self.to_vec();
        // let mut copy = Vec::with_capacity(self.len());
        // unsafe {
        //     copy.set_len(self.len());
        // }
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
            let mut sum = 0;
            for e in 0..256 {
                for a in 0..counts.len() {
                    let old_sum = sum;
                    sum += counts[a][e];
                    counts[a][e] = old_sum;
                }
            }
            src.par_chunks(cpu_workload)
                .zip(counts)
                .for_each(|(chunk, mut starts)| {
                    let mut buffer = [MaybeUninit::uninit(); BUFFER_SIZE * 256];
                    let mut buffer_starts: [usize; 256] = from_fn(|i| i * BUFFER_SIZE);
                    for e in chunk {
                        let d = e.get_digit(digit) as usize;
                        if buffer_starts[d] < (d + 1) * BUFFER_SIZE {
                            buffer[buffer_starts[d]].write(*e);
                            buffer_starts[d] += 1;
                        } else {
                            unsafe {
                                copy_nonoverlapping(
                                    buffer[d * BUFFER_SIZE].as_ptr(),
                                    &dst[starts[d]] as *const T as *mut T,
                                    BUFFER_SIZE,
                                );
                            }
                            starts[d] += BUFFER_SIZE;
                            buffer[d * BUFFER_SIZE].write(*e);
                            buffer_starts[d] = d * BUFFER_SIZE + 1;
                        }
                    }
                    for bin in 0..256 {
                        if buffer_starts[bin] - (bin * BUFFER_SIZE) > 0 {
                            unsafe {
                                copy_nonoverlapping(
                                    buffer[bin * BUFFER_SIZE].as_ptr(),
                                    &dst[starts[bin]] as *const T as *mut T,
                                    buffer_starts[bin] - (bin * BUFFER_SIZE),
                                );
                            }
                        }
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    //Rayon + derand (buffer sizes)
    fn radix_sort5(&mut self) {
        const BUFFER_SIZE: usize = 96;
        let num_cpus = current_num_threads() * 4;
        let cpu_workload = (self.len() + num_cpus - 1) / num_cpus;
        let mut copy = self.to_vec();
        // let mut copy = Vec::with_capacity(self.len());
        // unsafe {
        //     copy.set_len(self.len());
        // }
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
            let mut sum = 0;
            for e in 0..256 {
                for a in 0..counts.len() {
                    let old_sum = sum;
                    sum += counts[a][e];
                    counts[a][e] = old_sum;
                }
            }
            src.par_chunks(cpu_workload)
                .zip(counts)
                .for_each(|(chunk, mut starts)| {
                    let mut buffers = [MaybeUninit::uninit(); BUFFER_SIZE * 256];
                    let mut buffer_sizes = [0; 256];
                    for e in chunk {
                        let d = e.get_digit(digit) as usize;
                        if buffer_sizes[d] < BUFFER_SIZE {
                            buffers[d * BUFFER_SIZE + buffer_sizes[d]].write(*e);
                            buffer_sizes[d] += 1;
                        } else {
                            unsafe {
                                copy_nonoverlapping(
                                    buffers[d * BUFFER_SIZE].as_ptr() as *const T,
                                    &dst[starts[d]] as *const T as *mut T,
                                    BUFFER_SIZE,
                                );
                            }
                            starts[d] += BUFFER_SIZE;
                            buffers[d * BUFFER_SIZE].write(*e);
                            buffer_sizes[d] = 1;
                        }
                    }
                    for bin in 0..256 {
                        if buffer_sizes[bin] > 0 {
                            unsafe {
                                copy_nonoverlapping(
                                    buffers[bin * BUFFER_SIZE].as_ptr() as *const T,
                                    &dst[starts[bin]] as *const T as *mut T,
                                    buffer_sizes[bin],
                                );
                            }
                        }
                    }
                });
        }
        if T::DIGITS % 2 == 1 {
            self.swap_with_slice(copy.as_mut_slice());
        }
    }

    //Plain threads + derand
    fn radix_sort6(&mut self) {
        const BUFFER_SIZE: usize = 96;
        // const PAGE_SIZE: usize = 4096;
        let elements_per_cpu = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let mut temp = Vec::with_capacity(self.len());
        unsafe {
            // NOT PORTABLE
            let page_size = sysconf(_SC_PAGESIZE) as usize;
            temp.set_len(self.len());
            let as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            as_bytes
                .iter_mut()
                .step_by(page_size)
                .for_each(|element| *element = 0);
        }
        for current_digit_index in 0..T::DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (&*self, &*temp)
            } else {
                (&*temp, &*self)
            };
            let mut digit_histograms_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_cpu)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut digit_histogram = [0; 256];
                            for element in src_chunk {
                                digit_histogram[element.get_digit(current_digit_index) as usize] +=
                                    1;
                            }
                            digit_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle
                            .join()
                            .expect("failed to join count worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let digit_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for chunk_index in 0..digit_histograms_per_chunk.len() {
                        let old_prefix_sum = prefix_sum;
                        prefix_sum += digit_histograms_per_chunk[chunk_index][digit];
                        digit_histograms_per_chunk[chunk_index][digit] = old_prefix_sum;
                    }
                }
                digit_histograms_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_cpu)
                    .zip(digit_starts_per_chunk)
                    .for_each(|(src_chunk, mut digit_starts)| {
                        scope.spawn(move || {
                            let mut derand_buffers = [[MaybeUninit::uninit(); BUFFER_SIZE]; 256];
                            let mut derand_buffer_sizes = [0; 256];
                            for element in src_chunk {
                                let digit = element.get_digit(current_digit_index) as usize;
                                derand_buffers[digit][derand_buffer_sizes[digit]].write(*element);
                                derand_buffer_sizes[digit] += 1;
                                if derand_buffer_sizes[digit] == BUFFER_SIZE {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers[digit].as_ptr() as *const T,
                                            &dst[digit_starts[digit]] as *const T as *mut T,
                                            BUFFER_SIZE,
                                        );
                                    }
                                    digit_starts[digit] += BUFFER_SIZE;
                                    derand_buffer_sizes[digit] = 0;
                                }
                            }
                            for digit in 0..256 {
                                if derand_buffer_sizes[digit] > 0 {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers[digit].as_ptr() as *const T,
                                            &dst[digit_starts[digit]] as *const T as *mut T,
                                            derand_buffer_sizes[digit],
                                        );
                                    }
                                }
                            }
                        });
                    });
            });
        }
        if T::DIGITS % 2 == 1 {
            self.copy_from_slice(&mut temp);
        }
    }
}
