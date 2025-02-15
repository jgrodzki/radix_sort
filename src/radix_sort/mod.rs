pub use radix_digits::RadixDigits;
use rayon::{
    current_num_threads,
    iter::{IndexedParallelIterator, ParallelIterator},
    slice::ParallelSlice,
};
use std::{
    mem::{transmute, MaybeUninit},
    ptr::copy_nonoverlapping,
    slice::{self},
    thread::{self, available_parallelism},
};

mod radix_digits;
#[cfg(test)]
mod tests;

pub trait RadixSortCopyOnly<T>
where
    T: RadixDigits + Default + Copy,
{
    //Single thread
    fn radix_sort0(&mut self);
    //Thread per digit
    fn radix_sort1(&mut self);
    //Native threads
    fn radix_sort2(&mut self);
    //Partially initialized temp memory
    fn radix_sort3(&mut self);
    //Rayon
    fn radix_sort4(&mut self);
    //Buffering of writes
    fn radix_sort5(&mut self);
}

pub trait RadixSort<T: RadixDigits> {
    fn radix_sort(&mut self, cores: usize);
    fn radix_sort_big(&mut self);
}

pub fn counting_sort(data: &mut [u8]) {
    let mut temp = vec![0; data.len()];
    let mut bin_histogram = [0; 256];
    for element in data.as_ref() {
        bin_histogram[*element as usize] += 1;
    }
    let mut bin_starts = {
        bin_histogram.iter_mut().fold(0, |bin_start, bin_count| {
            let next_bin_start = bin_start + *bin_count;
            *bin_count = bin_start;
            next_bin_start
        });
        bin_histogram
    };
    for element in data.as_ref() {
        temp[bin_starts[*element as usize]] = *element;
        bin_starts[*element as usize] += 1;
    }
    data.copy_from_slice(&temp);
}

impl<T: RadixDigits> RadixSort<T> for [T] {
    fn radix_sort(&mut self, cores: usize) {
        const NUMBER_OF_THREADS: usize = 10;
        const BUFFER_SIZE: usize = 96;
        const PAGE_SIZE: usize = 4096;
        let elements_per_cpu = self.len().div_ceil(
            // available_parallelism()
            //     .expect("failed to acquire number of CPUs")
            //     .get(),
            cores,
        );
        let mut temp: Vec<MaybeUninit<T>> = Vec::with_capacity(self.len());
        unsafe {
            temp.set_len(self.len());
            let temp_as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            temp_as_bytes
                .iter_mut()
                .step_by(PAGE_SIZE)
                .for_each(|element| *element = 0);
        }
        let temp_slice = unsafe { transmute(temp.as_slice()) };
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp_slice)
            } else {
                (temp_slice, self.as_ref())
            };
            let mut bin_histogram_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_cpu)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut bin_histogram = [0; 256];
                            for element in src_chunk {
                                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                            }
                            bin_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle.join().expect("failed to join worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_cpu)
                    .zip(bin_starts_per_chunk)
                    .for_each(|(src_chunk, mut bin_starts)| {
                        scope.spawn(move || {
                            let mut derand_buffers =
                                MaybeUninit::<[[T; BUFFER_SIZE]; 256]>::uninit();
                            let derand_buffers_slice = unsafe { derand_buffers.assume_init_mut() };
                            let mut derand_buffer_sizes = [0; 256];
                            for element in src_chunk {
                                let digit_value = element.get_digit(current_digit_index) as usize;
                                unsafe {
                                    copy_nonoverlapping(
                                        element,
                                        &derand_buffers_slice[digit_value]
                                            [derand_buffer_sizes[digit_value]]
                                            as *const T
                                            as *mut T,
                                        1,
                                    );
                                }
                                derand_buffer_sizes[digit_value] += 1;
                                if derand_buffer_sizes[digit_value] == BUFFER_SIZE {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit_value].as_ptr(),
                                            &dst[bin_starts[digit_value]] as *const T as *mut T,
                                            BUFFER_SIZE,
                                        );
                                    }
                                    bin_starts[digit_value] += BUFFER_SIZE;
                                    derand_buffer_sizes[digit_value] = 0;
                                }
                            }
                            for digit in 0..256 {
                                if derand_buffer_sizes[digit] > 0 {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit].as_ptr(),
                                            &dst[bin_starts[digit]] as *const T as *mut T,
                                            derand_buffer_sizes[digit],
                                        );
                                    }
                                }
                            }
                        });
                    });
            });
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            unsafe {
                copy_nonoverlapping(temp_slice.as_ptr(), self.as_mut_ptr(), self.len());
            }
        }
    }

    fn radix_sort_big(&mut self) {
        const PAGE_SIZE: usize = 4096;
        const BUFFER_SIZE: usize = 96;
        let elements_per_cpu = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let mut copy: Vec<MaybeUninit<T>> = Vec::with_capacity(self.len());
        unsafe {
            copy.set_len(self.len());
            copy_nonoverlapping(self.as_ptr(), copy.as_mut_ptr() as *mut T, self.len());
        }
        let copy_slice: &[T] = unsafe { transmute(copy.as_slice()) };
        let temp1 = copy_slice
            .iter()
            .map(|e| e as *const T as usize)
            .collect::<Vec<_>>();
        let mut temp2: Vec<MaybeUninit<usize>> = Vec::with_capacity(self.len());
        unsafe {
            temp2.set_len(self.len());
            let temp2_as_bytes = slice::from_raw_parts_mut(
                temp2.as_mut_ptr() as *mut u8,
                self.len() * size_of::<usize>(),
            );
            temp2_as_bytes
                .iter_mut()
                .step_by(PAGE_SIZE)
                .for_each(|element| *element = 0);
        }
        let temp2_slice = unsafe { transmute(temp2.as_slice()) };
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (temp1.as_slice(), temp2_slice)
            } else {
                (temp2_slice, temp1.as_slice())
            };
            let mut bin_histogram_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_cpu)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut bin_histogram = [0; 256];
                            for element in src_chunk
                                .iter()
                                .map(|e| unsafe { (*e as *const T).as_ref().unwrap() })
                            {
                                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                            }
                            bin_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle.join().expect("failed to join worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_cpu)
                    .zip(bin_starts_per_chunk)
                    .for_each(|(src_chunk, mut bin_starts)| {
                        scope.spawn(move || {
                            let mut derand_buffers =
                                MaybeUninit::<[[*const T; BUFFER_SIZE]; 256]>::uninit();
                            let derand_buffers_slice = unsafe { derand_buffers.assume_init_mut() };
                            let mut derand_buffer_sizes = [0; 256];
                            for element in src_chunk
                                .iter()
                                .map(|e| unsafe { (*e as *const T).as_ref().unwrap() })
                            {
                                let digit_value = element.get_digit(current_digit_index) as usize;
                                derand_buffers_slice[digit_value]
                                    [derand_buffer_sizes[digit_value]] = element;
                                derand_buffer_sizes[digit_value] += 1;
                                if derand_buffer_sizes[digit_value] == BUFFER_SIZE {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit_value].as_ptr(),
                                            &dst[bin_starts[digit_value]] as *const usize
                                                as *mut *const T,
                                            BUFFER_SIZE,
                                        );
                                    }
                                    bin_starts[digit_value] += BUFFER_SIZE;
                                    derand_buffer_sizes[digit_value] = 0;
                                }
                            }
                            for digit in 0..256 {
                                if derand_buffer_sizes[digit] > 0 {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit].as_ptr(),
                                            &dst[bin_starts[digit]] as *const usize
                                                as *mut *const T,
                                            derand_buffer_sizes[digit],
                                        );
                                    }
                                }
                            }
                        });
                    });
            });
        }
        let src = if T::NUMBER_OF_DIGITS % 2 == 0 {
            temp1.as_slice()
        } else {
            temp2_slice
        };
        for (a, b) in self.iter_mut().zip(src) {
            unsafe {
                copy_nonoverlapping(*b as *const T, a, 1);
            }
        }
    }
}

impl<T> RadixSortCopyOnly<T> for [T]
where
    T: RadixDigits + Default + Copy,
{
    //Single thread
    fn radix_sort0(&mut self) {
        let mut temp = vec![T::default(); self.len()];
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp.as_mut_slice())
            } else {
                (temp.as_slice(), self.as_mut())
            };
            let mut bin_histogram = [0; 256];
            for element in src {
                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
            }
            let mut bin_starts = {
                bin_histogram.iter_mut().fold(0, |bin_start, bin_count| {
                    let next_bin_start = bin_start + *bin_count;
                    *bin_count = bin_start;
                    next_bin_start
                });
                bin_histogram
            };
            for element in src {
                let digit_value = element.get_digit(current_digit_index) as usize;
                dst[bin_starts[digit_value]] = *element;
                bin_starts[digit_value] += 1;
            }
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }

    //Thread per digit
    fn radix_sort1(&mut self) {
        let mut temp = vec![T::default(); self.len()];
        let bin_starts_per_digit = thread::scope(|scope| {
            let data_ref = self.as_ref();
            let workers = (0..T::NUMBER_OF_DIGITS)
                .map(|digit_index| {
                    scope.spawn(move || {
                        let mut bin_histogram = vec![0; 256];
                        for element in data_ref {
                            bin_histogram[element.get_digit(digit_index) as usize] += 1;
                        }
                        bin_histogram.iter_mut().fold(0, |bin_start, bin_count| {
                            let next_bin_start = bin_start + *bin_count;
                            *bin_count = bin_start;
                            next_bin_start
                        });
                        bin_histogram
                    })
                })
                .collect::<Vec<_>>();
            workers
                .into_iter()
                .map(|thread_handle| thread_handle.join().expect("failed to join worker thread"))
                .collect::<Vec<_>>()
        });
        for (digit_index, mut bin_starts) in bin_starts_per_digit.into_iter().enumerate() {
            let (src, dst) = if digit_index % 2 == 0 {
                (self.as_ref(), temp.as_mut_slice())
            } else {
                (temp.as_slice(), self.as_mut())
            };
            for element in src {
                let digit_value = element.get_digit(digit_index as u8) as usize;
                dst[bin_starts[digit_value]] = *element;
                bin_starts[digit_value] += 1;
            }
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }

    //Native threads
    fn radix_sort2(&mut self) {
        let elements_per_chunk = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let temp = vec![T::default(); self.len()];
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp.as_slice())
            } else {
                (temp.as_slice(), self.as_ref())
            };
            let mut bin_histogram_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_chunk)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut bin_histogram = [0; 256];
                            for element in src_chunk {
                                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                            }
                            bin_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle.join().expect("failed to join worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_chunk)
                    .zip(bin_starts_per_chunk)
                    .for_each(|(src_chunk, mut bin_starts)| {
                        scope.spawn(move || {
                            for element in src_chunk {
                                let digit_value =
                                    element.get_digit(current_digit_index as u8) as usize;
                                let dst = unsafe {
                                    slice::from_raw_parts_mut(dst.as_ptr() as *mut T, dst.len())
                                };
                                dst[bin_starts[digit_value]] = *element;
                                bin_starts[digit_value] += 1;
                            }
                        });
                    });
            });
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }

    //Partially initialized temp memory
    fn radix_sort3(&mut self) {
        const PAGE_SIZE: usize = 4096;
        let elements_per_chunk = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let mut temp = Vec::with_capacity(self.len());
        unsafe {
            temp.set_len(self.len());
            let temp_as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            temp_as_bytes
                .iter_mut()
                .step_by(PAGE_SIZE)
                .for_each(|element| *element = 0);
        }
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp.as_slice())
            } else {
                (temp.as_slice(), self.as_ref())
            };
            let mut bin_histogram_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_chunk)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut bin_histogram = [0; 256];
                            for element in src_chunk {
                                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                            }
                            bin_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle.join().expect("failed to join worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_chunk)
                    .zip(bin_starts_per_chunk)
                    .for_each(|(src_chunk, mut bin_starts)| {
                        scope.spawn(move || {
                            for element in src_chunk {
                                let digit_value =
                                    element.get_digit(current_digit_index as u8) as usize;
                                let dst = unsafe {
                                    slice::from_raw_parts_mut(dst.as_ptr() as *mut T, dst.len())
                                };
                                dst[bin_starts[digit_value]] = *element;
                                bin_starts[digit_value] += 1;
                            }
                        });
                    });
            });
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }

    //Rayon
    fn radix_sort4(&mut self) {
        const PAGE_SIZE: usize = 4096;
        const CHUNK_MULTIPLIER: usize = 2;
        let elements_per_chunk = self
            .len()
            .div_ceil(current_num_threads() * CHUNK_MULTIPLIER);
        let mut temp = Vec::with_capacity(self.len());
        unsafe {
            temp.set_len(self.len());
            let temp_as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            temp_as_bytes
                .iter_mut()
                .step_by(PAGE_SIZE)
                .for_each(|element| *element = 0);
        }
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp.as_slice())
            } else {
                (temp.as_slice(), self.as_ref())
            };
            let mut bin_histogram_per_chunk = src
                .par_chunks(elements_per_chunk)
                .map(|src_chunk| {
                    let mut bin_histogram = [0; 256];
                    for element in src_chunk {
                        bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                    }
                    bin_histogram
                })
                .collect::<Vec<_>>();
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            src.par_chunks(elements_per_chunk)
                .zip(bin_starts_per_chunk)
                .for_each(|(src_chunk, mut bin_starts)| {
                    for element in src_chunk {
                        let digit_value = element.get_digit(current_digit_index as u8) as usize;
                        let dst =
                            unsafe { slice::from_raw_parts_mut(dst.as_ptr() as *mut T, dst.len()) };
                        dst[bin_starts[digit_value]] = *element;
                        bin_starts[digit_value] += 1;
                    }
                });
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }

    //Buffering of writes
    fn radix_sort5(&mut self) {
        const BUFFER_SIZE: usize = 96;
        const PAGE_SIZE: usize = 4096;
        let elements_per_chunk = self.len().div_ceil(
            available_parallelism()
                .expect("failed to acquire number of CPUs")
                .get(),
        );
        let mut temp = Vec::with_capacity(self.len());
        unsafe {
            temp.set_len(self.len());
            let temp_as_bytes = slice::from_raw_parts_mut(
                temp.as_mut_ptr() as *mut u8,
                self.len() * size_of::<T>(),
            );
            temp_as_bytes
                .iter_mut()
                .step_by(PAGE_SIZE)
                .for_each(|element| *element = 0);
        }
        for current_digit_index in 0..T::NUMBER_OF_DIGITS {
            let (src, dst) = if current_digit_index % 2 == 0 {
                (self.as_ref(), temp.as_slice())
            } else {
                (temp.as_slice(), self.as_ref())
            };
            let mut bin_histogram_per_chunk = thread::scope(|scope| {
                let workers = src
                    .chunks(elements_per_chunk)
                    .map(|src_chunk| {
                        scope.spawn(move || {
                            let mut bin_histogram = [0; 256];
                            for element in src_chunk {
                                bin_histogram[element.get_digit(current_digit_index) as usize] += 1;
                            }
                            bin_histogram
                        })
                    })
                    .collect::<Vec<_>>();
                workers
                    .into_iter()
                    .map(|thread_handle| {
                        thread_handle.join().expect("failed to join worker thread")
                    })
                    .collect::<Vec<_>>()
            });
            let bin_starts_per_chunk = {
                let mut prefix_sum = 0;
                for digit in 0..256 {
                    for bin_histogram in &mut bin_histogram_per_chunk {
                        let new_prefix_sum = prefix_sum + bin_histogram[digit];
                        bin_histogram[digit] = prefix_sum;
                        prefix_sum = new_prefix_sum;
                    }
                }
                bin_histogram_per_chunk
            };
            thread::scope(|scope| {
                src.chunks(elements_per_chunk)
                    .zip(bin_starts_per_chunk)
                    .for_each(|(src_chunk, mut bin_starts)| {
                        scope.spawn(move || {
                            let mut derand_buffers =
                                MaybeUninit::<[[T; BUFFER_SIZE]; 256]>::uninit();
                            let derand_buffers_slice = unsafe { derand_buffers.assume_init_mut() };
                            let mut derand_buffer_sizes = [0; 256];
                            for element in src_chunk {
                                let digit_value = element.get_digit(current_digit_index) as usize;
                                derand_buffers_slice[digit_value]
                                    [derand_buffer_sizes[digit_value]] = *element;
                                derand_buffer_sizes[digit_value] += 1;
                                if derand_buffer_sizes[digit_value] == BUFFER_SIZE {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit_value].as_ptr() as *const T,
                                            &dst[bin_starts[digit_value]] as *const T as *mut T,
                                            BUFFER_SIZE,
                                        );
                                    }
                                    bin_starts[digit_value] += BUFFER_SIZE;
                                    derand_buffer_sizes[digit_value] = 0;
                                }
                            }
                            for digit in 0..256 {
                                if derand_buffer_sizes[digit] > 0 {
                                    unsafe {
                                        copy_nonoverlapping(
                                            derand_buffers_slice[digit].as_ptr() as *const T,
                                            &dst[bin_starts[digit]] as *const T as *mut T,
                                            derand_buffer_sizes[digit],
                                        );
                                    }
                                }
                            }
                        });
                    });
            });
        }
        if T::NUMBER_OF_DIGITS % 2 == 1 {
            self.copy_from_slice(&temp);
        }
    }
}
