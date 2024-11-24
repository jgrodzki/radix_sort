use super::RadixSort;
use rand::Rng;

fn verify_sorted<T>(data: &[T])
where
    T: Clone + Ord,
{
    let mut sorted_data = data.to_owned();
    sorted_data.sort();
    if data != sorted_data {
        panic!("Not sorted properly!");
    }
}

// Radix sort 6

#[test]
fn radix_sort6_u8() {
    let mut data = vec![0u8; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_u16() {
    let mut data = vec![0u16; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_u32() {
    let mut data = vec![0u32; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_u64() {
    let mut data = vec![0u64; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_u128() {
    let mut data = vec![0u128; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_usize() {
    let mut data = vec![0usize; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_i8() {
    let mut data = vec![0i8; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_i16() {
    let mut data = vec![0i16; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_i32() {
    let mut data = vec![0i32; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_i64() {
    let mut data = vec![0i64; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_i128() {
    let mut data = vec![0i128; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

#[test]
fn radix_sort6_isize() {
    let mut data = vec![0isize; 1e6 as usize];
    rand::thread_rng().fill(data.as_mut_slice());
    data.radix_sort6();
    verify_sorted(&data);
}

// float tests are manual since float types don't implement Eq or Ord
// floats are sorted according to bit representation
#[test]
fn radix_sort6_f32() {
    let mut data = [
        0.39610386,
        0.36372882,
        0.0,
        -0.0,
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.4200853,
        -0.38027912,
        0.31958795,
    ];
    data.radix_sort6();
    let sorted_data = [
        f32::NEG_INFINITY,
        -0.4200853,
        -0.38027912,
        -0.0,
        0.0,
        0.31958795,
        0.36372882,
        0.39610386,
        f32::INFINITY,
        f32::NAN,
    ];
    if format!("{data:?}") != format!("{sorted_data:?}") {
        panic!("Not sorted properly!");
    }
}

#[test]
fn radix_sort6_f64() {
    let mut data = [
        -0.134055627892947,
        0.1076179532413728,
        0.0,
        -0.0,
        f64::NAN,
        f64::INFINITY,
        f64::NEG_INFINITY,
        -0.08097993397227343,
        0.33448000141095235,
        0.45875483155949237,
    ];
    data.radix_sort6();
    let sorted_data = [
        f64::NEG_INFINITY,
        -0.134055627892947,
        -0.08097993397227343,
        -0.0,
        0.0,
        0.1076179532413728,
        0.33448000141095235,
        0.45875483155949237,
        f64::INFINITY,
        f64::NAN,
    ];
    if format!("{data:?}") != format!("{sorted_data:?}") {
        panic!("Not sorted properly!");
    }
}
