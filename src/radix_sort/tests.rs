use core::f32;
use std::cmp::Ordering;

use rand::{seq::SliceRandom, thread_rng, Rng};
use rand_distr::{Distribution, Standard, Uniform};

use super::RadixSort;

fn verify_sorted<T>(data: &[T], original: Option<&mut [T]>)
where
    T: Clone + Ord,
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

#[test]
fn radix_sort_u8() {
    let mut data_original = vec![0u8; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_u16() {
    let mut data_original = vec![0u16; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_u32() {
    let mut data_original = vec![0u32; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_u64() {
    let mut data_original = vec![0u64; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_u128() {
    let mut data_original = vec![0u128; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_usize() {
    let mut data_original = vec![0usize; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_i8() {
    let mut data_original = vec![0i8; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_i16() {
    let mut data_original = vec![0i16; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_i32() {
    let mut data_original = vec![0i32; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_i64() {
    let mut data_original = vec![0i64; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_i128() {
    let mut data_original = vec![0i128; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_isize() {
    let mut data_original = vec![0isize; 1e6 as usize];
    rand::thread_rng().fill(data_original.as_mut_slice());
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    verify_sorted(&data_sorted, Some(&mut data_original));
}

#[test]
fn radix_sort_f32() {
    let mut data_original = Uniform::new(-1.0, 1.0)
        .sample_iter(thread_rng())
        .take(1e6 as usize)
        .collect::<Vec<_>>();
    *data_original.choose_mut(&mut thread_rng()).unwrap() = 0.0;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = -0.0;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f32::NAN;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f32::INFINITY;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f32::NEG_INFINITY;
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    data_original.sort_by(|a, b| a.total_cmp(b));
    for (a, b) in data_sorted.into_iter().zip(data_original) {
        if a.total_cmp(&b) != Ordering::Equal {
            panic!("Not sorted properly!");
        }
    }
}

#[test]
fn radix_sort_f64() {
    let mut data_original = Uniform::new(-1.0, 1.0)
        .sample_iter(thread_rng())
        .take(1e6 as usize)
        .collect::<Vec<_>>();
    *data_original.choose_mut(&mut thread_rng()).unwrap() = 0.0;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = -0.0;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f64::NAN;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f64::INFINITY;
    *data_original.choose_mut(&mut thread_rng()).unwrap() = f64::NEG_INFINITY;
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    data_original.sort_by(|a, b| a.total_cmp(b));
    for (a, b) in data_sorted.into_iter().zip(data_original) {
        if a.total_cmp(&b) != Ordering::Equal {
            panic!("Not sorted properly!");
        }
    }
}

#[test]
fn radix_sort_tuple() {
    let mut data_original = Standard
        .sample_iter(thread_rng())
        .take(1e6 as usize)
        .collect::<Vec<(u32, u32)>>();
    let mut data_sorted = data_original.clone();
    data_sorted.radix_sort();
    data_original.sort_by_key(|e| e.0);
    if data_sorted != data_original {
        panic!("Not sorted properly!");
    }
}
