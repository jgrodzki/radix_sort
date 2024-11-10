use std::sync::mpsc::channel;

use arbitrary_chunks::ArbitraryChunks;
use rayon::{
    iter::{IndexedParallelIterator, IntoParallelIterator, ParallelIterator},
    slice::{ParallelSlice, ParallelSliceMut},
};

fn mt_lsb_sort<T>(
    src_bucket: &mut [T],
    dst_bucket: &mut [T],
    tile_counts: &[[usize; 256]],
    tile_size: usize,
    level: usize,
) where
    T: rdst::RadixKey + Sized + Send + Copy + Sync,
{
    let tiles = tile_counts.len();
    let mut minor_counts = Vec::with_capacity(256 * tiles);

    for b in 0..256 {
        for tile in tile_counts.iter() {
            minor_counts.push(tile[b]);
        }
    }

    let mut chunks: Vec<&mut [T]> = dst_bucket.arbitrary_chunks_mut(&minor_counts).collect();
    chunks.reverse();

    let mut collated_chunks: Vec<Vec<&mut [T]>> = Vec::with_capacity(tiles);
    collated_chunks.resize_with(tiles, Vec::new);

    for _ in 0..256 {
        for coll_chunk in collated_chunks.iter_mut().take(tiles) {
            coll_chunk.push(chunks.pop().unwrap());
        }
    }

    collated_chunks
        .into_par_iter()
        .zip(src_bucket.par_chunks(tile_size))
        .for_each(|(mut buckets, bucket)| {
            if bucket.is_empty() {
                return;
            }

            let mut offsets = [0usize; 256];
            let mut ends = [0usize; 256];

            for (i, b) in buckets.iter().enumerate() {
                if b.is_empty() {
                    continue;
                }

                ends[i] = b.len() - 1;
            }

            let mut left = 0;
            let mut right = bucket.len() - 1;
            let pre = bucket.len() % 8;

            for _ in 0..pre {
                let b = bucket[right].get_level(level) as usize;

                buckets[b][ends[b]] = bucket[right];
                ends[b] = ends[b].wrapping_sub(1);
                right = right.saturating_sub(1);
            }

            if pre == bucket.len() {
                return;
            }

            let end = (bucket.len() - pre) / 2;

            while left < end {
                let bl_0 = bucket[left].get_level(level) as usize;
                let bl_1 = bucket[left + 1].get_level(level) as usize;
                let bl_2 = bucket[left + 2].get_level(level) as usize;
                let bl_3 = bucket[left + 3].get_level(level) as usize;
                let br_0 = bucket[right].get_level(level) as usize;
                let br_1 = bucket[right - 1].get_level(level) as usize;
                let br_2 = bucket[right - 2].get_level(level) as usize;
                let br_3 = bucket[right - 3].get_level(level) as usize;

                buckets[bl_0][offsets[bl_0]] = bucket[left];
                offsets[bl_0] += 1;
                buckets[br_0][ends[br_0]] = bucket[right];
                ends[br_0] = ends[br_0].wrapping_sub(1);
                buckets[bl_1][offsets[bl_1]] = bucket[left + 1];
                offsets[bl_1] += 1;
                buckets[br_1][ends[br_1]] = bucket[right - 1];
                ends[br_1] = ends[br_1].wrapping_sub(1);
                buckets[bl_2][offsets[bl_2]] = bucket[left + 2];
                offsets[bl_2] += 1;
                buckets[br_2][ends[br_2]] = bucket[right - 2];
                ends[br_2] = ends[br_2].wrapping_sub(1);
                buckets[bl_3][offsets[bl_3]] = bucket[left + 3];
                offsets[bl_3] += 1;
                buckets[br_3][ends[br_3]] = bucket[right - 3];
                ends[br_3] = ends[br_3].wrapping_sub(1);

                left += 4;
                right = right.wrapping_sub(4);
            }
        });
}

pub fn mt_lsb_sort_adapter<T>(
    bucket: &mut [T],
    start_level: usize,
    end_level: usize,
    tile_size: usize,
) where
    T: rdst::RadixKey + Sized + Send + Copy + Sync,
{
    if bucket.len() < 2 {
        return;
    }

    let mut tmp_bucket = get_tmp_bucket(bucket.len());
    let levels: Vec<usize> = (start_level..=end_level).collect();
    let mut invert = false;

    for level in levels {
        let (tile_counts, already_sorted) = if invert {
            get_tile_counts(&tmp_bucket, tile_size, level)
        } else {
            get_tile_counts(bucket, tile_size, level)
        };

        if already_sorted {
            continue;
        }

        if invert {
            mt_lsb_sort(&mut tmp_bucket, bucket, &tile_counts, tile_size, level)
        } else {
            mt_lsb_sort(bucket, &mut tmp_bucket, &tile_counts, tile_size, level)
        };

        invert = !invert;
    }

    if invert {
        bucket
            .par_chunks_mut(tile_size)
            .zip(tmp_bucket.par_chunks(tile_size))
            .for_each(|(chunk, tmp_chunk)| {
                chunk.copy_from_slice(tmp_chunk);
            });
    }
}

fn get_tmp_bucket<T>(len: usize) -> Vec<T> {
    let mut tmp_bucket = Vec::with_capacity(len);
    unsafe {
        // Safety: This will leave the vec with potentially uninitialized data
        // however as we account for every value when placing things
        // into tmp_bucket, this is "safe". This is used because it provides a
        // very significant speed improvement over resize, to_vec etc.
        tmp_bucket.set_len(len);
    }

    tmp_bucket
}

fn get_tile_counts<T>(bucket: &[T], tile_size: usize, level: usize) -> (Vec<[usize; 256]>, bool)
where
    T: rdst::RadixKey + Copy + Sized + Send + Sync,
{
    let tiles: Vec<([usize; 256], bool, u8, u8)> = bucket
        .par_chunks(tile_size)
        .map(|chunk| par_get_counts_with_ends(chunk, level))
        .collect();

    let mut all_sorted = true;

    if tiles.len() == 1 {
        // If there is only one tile, we already have a flag for if it is sorted
        all_sorted = tiles[0].1;
    } else {
        // Check if any of the tiles, or any of the tile boundaries are unsorted
        for tile in tiles.windows(2) {
            if !tile[0].1 || !tile[1].1 || tile[1].2 < tile[0].3 {
                all_sorted = false;
                break;
            }
        }
    }

    (tiles.into_iter().map(|v| v.0).collect(), all_sorted)
}

fn par_get_counts_with_ends<T>(bucket: &[T], level: usize) -> ([usize; 256], bool, u8, u8)
where
    T: rdst::RadixKey + Sized + Send + Sync,
{
    if bucket.len() < 400_000 {
        return get_counts_with_ends(bucket, level);
    }

    let threads = rayon::current_num_threads();
    let chunk_divisor = 8;
    let chunk_size = (bucket.len() / threads / chunk_divisor) + 1;
    let chunks = bucket.par_chunks(chunk_size);
    let len = chunks.len();
    let (tx, rx) = channel();

    chunks.enumerate().for_each_with(tx, |tx, (i, chunk)| {
        let counts = get_counts_with_ends(chunk, level);
        tx.send((i, counts.0, counts.1, counts.2, counts.3))
            .unwrap();
    });

    let mut msb_counts = [0usize; 256];
    let mut already_sorted = true;
    let mut boundaries = vec![(0u8, 0u8); len];

    for _ in 0..len {
        let (i, counts, chunk_sorted, start, end) = rx.recv().unwrap();

        if !chunk_sorted {
            already_sorted = false;
        }

        boundaries[i].0 = start;
        boundaries[i].1 = end;

        for (i, c) in counts.iter().enumerate() {
            msb_counts[i] += *c;
        }
    }

    // Check the boundaries of each counted chunk, to see if the full bucket
    // is already sorted
    if already_sorted {
        for w in boundaries.windows(2) {
            if w[1].0 < w[0].1 {
                already_sorted = false;
                break;
            }
        }
    }

    (
        msb_counts,
        already_sorted,
        boundaries[0].0,
        boundaries[boundaries.len() - 1].1,
    )
}

fn get_counts_with_ends<T>(bucket: &[T], level: usize) -> ([usize; 256], bool, u8, u8)
where
    T: rdst::RadixKey,
{
    let mut already_sorted = true;
    let mut continue_from = bucket.len();
    let mut counts_1 = [0usize; 256];
    let mut last = 0usize;

    for (i, item) in bucket.iter().enumerate() {
        let b = item.get_level(level) as usize;
        counts_1[b] += 1;

        if b < last {
            continue_from = i + 1;
            already_sorted = false;
            break;
        }

        last = b;
    }

    if continue_from == bucket.len() {
        return (
            counts_1,
            already_sorted,
            bucket[0].get_level(level),
            last as u8,
        );
    }

    let mut counts_2 = [0usize; 256];
    let mut counts_3 = [0usize; 256];
    let mut counts_4 = [0usize; 256];
    let chunks = bucket[continue_from..].chunks_exact(4);
    let rem = chunks.remainder();

    chunks.into_iter().for_each(|chunk| {
        let a = chunk[0].get_level(level) as usize;
        let b = chunk[1].get_level(level) as usize;
        let c = chunk[2].get_level(level) as usize;
        let d = chunk[3].get_level(level) as usize;

        counts_1[a] += 1;
        counts_2[b] += 1;
        counts_3[c] += 1;
        counts_4[d] += 1;
    });

    rem.iter().for_each(|v| {
        let b = v.get_level(level) as usize;
        counts_1[b] += 1;
    });

    for i in 0..256 {
        counts_1[i] += counts_2[i];
        counts_1[i] += counts_3[i];
        counts_1[i] += counts_4[i];
    }

    let b_first = bucket.first().unwrap().get_level(level);
    let b_last = bucket.last().unwrap().get_level(level);

    (counts_1, already_sorted, b_first, b_last)
}
