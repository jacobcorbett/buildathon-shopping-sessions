use crate::ids::{Key, Names};
use ahash::RandomState;
use memchr::memchr;
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use std::{fs, io, path::Path};

pub type Id<'a> = &'a [u8];
pub const DAY: i64 = 86_400_000;
pub const WEEK: i64 = 7 * DAY;
pub const PARTITIONS: usize = 64;
const CHUNK_BYTES: usize = 4 * 1024 * 1024;

pub fn map(path: &Path) -> io::Result<Mmap> {
    let file = fs::File::open(path)?;
    // The scorer mounts inputs read-only and keeps them unchanged during the run.
    unsafe { MmapOptions::new().map(&file) }
}

pub fn map_dir(path: &Path) -> io::Result<Vec<Mmap>> {
    let mut paths = Vec::new();
    for entry in fs::read_dir(path)? {
        let path = entry?.path();
        if path.extension().is_some_and(|ext| ext == "csv") {
            paths.push(path);
        }
    }
    paths.sort_unstable();
    paths.iter().map(|path| map(path)).collect()
}

pub fn lines(bytes: &[u8]) -> impl Iterator<Item = &[u8]> {
    bytes.split(|&byte| byte == b'\n').skip(1)
}

#[inline]
pub fn fields<const N: usize>(mut line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if line.len() >= 32 && line.len() <= 96 {
        return vector_fields(line);
    }
    let mut result = [&[][..]; N];
    for (index, field) in result.iter_mut().enumerate() {
        let raw = if index + 1 == N {
            if memchr(b',', line).is_some() {
                return None;
            }
            line
        } else {
            let end = memchr(b',', line)?;
            let raw = &line[..end];
            line = &line[end + 1..];
            raw
        };
        let value = if let [b'"', value @ .., b'"'] = raw {
            value
        } else {
            raw
        };
        if memchr(b'"', value).is_some() {
            return None;
        }
        *field = value;
    }
    Some(result)
}

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn vector_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use std::arch::x86_64::*;
    let masks = |offset: usize| {
        // The two end blocks can overlap. Both loads stay within this line.
        unsafe {
            let block = _mm256_loadu_si256(line.as_ptr().add(offset).cast());
            let mask = |byte| {
                _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, _mm256_set1_epi8(byte))) as u32
            };
            (mask(b',' as i8), mask(b'"' as i8))
        }
    };
    let (first, quotes) = masks(0);
    let mut commas = u128::from(first);
    let mut quotes = u128::from(quotes);
    if line.len() > 64 {
        let (middle, q) = masks(32);
        commas |= u128::from(middle) << 32;
        quotes |= u128::from(q) << 32;
    }
    let tail = line.len() - 32;
    let (last, q) = masks(tail);
    commas |= u128::from(last) << tail;
    quotes |= u128::from(q) << tail;
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    commas |= 1 << line.len();
    let mut start = 0;
    let mut result = [&[][..]; N];
    for field in &mut result {
        let end = commas.trailing_zeros() as usize;
        commas &= commas - 1;
        *field = if quotes & (1 << start) != 0 {
            if end < start + 2 || quotes & (1 << (end - 1)) == 0 {
                return None;
            }
            quotes ^= (1 << start) | (1 << (end - 1));
            &line[start + 1..end - 1]
        } else {
            &line[start..end]
        };
        start = end + 1;
    }
    (quotes == 0).then_some(result)
}

#[inline]
fn digit(byte: u8) -> Option<u32> {
    let value = byte.wrapping_sub(b'0');
    (value <= 9).then_some(u32::from(value))
}

#[inline]
pub fn number(bytes: &[u8]) -> Option<u64> {
    if bytes.is_empty() {
        return None;
    }
    let mut value = 0_u64;
    // Up to 19 decimal digits always fit in u64.
    if bytes.len() < 20 {
        for &byte in bytes {
            value = value * 10 + u64::from(digit(byte)?);
        }
    } else {
        for &byte in bytes {
            value = value
                .checked_mul(10)?
                .checked_add(u64::from(digit(byte)?))?;
        }
    }
    Some(value)
}

#[inline]
fn pair(bytes: &[u8], start: usize) -> Option<u32> {
    Some(digit(bytes[start])? * 10 + digit(bytes[start + 1])?)
}

#[inline]
fn ascii_calendar(bytes: &[u8]) -> Option<i64> {
    // Parsed days recur across both event and ingest timestamps. Use the
    // entire date as the tag so out-of-window and malformed dates stay exact.
    thread_local! {
        static DAYS: [std::cell::Cell<([u8; 10], i64)>; 32] =
            const { [const { std::cell::Cell::new(([0; 10], 0)) }; 32] };
    }
    let date: [u8; 10] = bytes[..10].try_into().unwrap();
    let index = (usize::from(date[8]) * 10 + usize::from(date[9])) & 31;
    let midnight = DAYS.with(|slots| {
        let (tag, value) = slots[index].get();
        if tag == date {
            return Some(value);
        }
        let value = calendar_day(bytes)?;
        slots[index].set((date, value));
        Some(value)
    })?;
    if bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let hour = pair(bytes, 11)?;
    let minute = pair(bytes, 14)?;
    let second = pair(bytes, 17)?;
    if hour > 23 || minute > 59 || second > 59 {
        return None;
    }
    Some(midnight + i64::from((hour * 60 + minute) * 60 + second) * 1000)
}

#[inline]
fn calendar_day(bytes: &[u8]) -> Option<i64> {
    if bytes[7] != b'-' {
        return None;
    }
    let year = pair(bytes, 0)? * 100 + pair(bytes, 2)?;
    let month = pair(bytes, 5)?;
    let day = pair(bytes, 8)?;
    if !(1..=12).contains(&month) || day == 0 {
        return None;
    }
    let leap = year.is_multiple_of(4) && (!year.is_multiple_of(100) || year.is_multiple_of(400));
    let month_lengths = [
        31,
        28 + u32::from(leap),
        31,
        30,
        31,
        30,
        31,
        31,
        30,
        31,
        30,
        31,
    ];
    if day > month_lengths[month as usize - 1] {
        return None;
    }
    // Gregorian days since the Unix epoch, with March as the first month
    // so that leap day is at the end of a year.
    let year = i64::from(year) - i64::from(month <= 2);
    let era = year.div_euclid(400);
    let year_in_era = year - era * 400;
    let march_month = if month > 2 { month - 3 } else { month + 9 };
    let day_in_year = i64::from((153 * march_month + 2) / 5 + day - 1);
    let days = era * 146097 + year_in_era * 365 + year_in_era / 4 - year_in_era / 100 + day_in_year
        - 719468;
    Some(days * DAY)
}

#[inline]
pub fn timestamp(bytes: &[u8]) -> Option<i64> {
    if !matches!(bytes.len(), 20 | 25) || bytes[4] != b'-' {
        return i64::try_from(number(bytes)?).ok();
    }
    let local = ascii_calendar(bytes)?;
    let offset = match (bytes.len(), bytes[19]) {
        (20, b'Z') => 0,
        (25, sign @ (b'+' | b'-')) if bytes[22] == b':' => {
            let hours = pair(bytes, 20)?;
            let minutes = pair(bytes, 23)?;
            if hours > 23 || minutes > 59 {
                return None;
            }
            i64::from(hours * 60 + minutes) * 60_000 * if sign == b'+' { 1 } else { -1 }
        }
        _ => return None,
    };
    Some(local - offset)
}

pub fn chunks(files: &[Mmap]) -> Vec<&[u8]> {
    sized_chunks(files, CHUNK_BYTES)
}

fn sized_chunks(files: &[Mmap], bytes: usize) -> Vec<&[u8]> {
    let mut chunks = Vec::new();
    for file in files {
        let mut remainder = match memchr(b'\n', file) {
            Some(header_end) => &file[header_end + 1..],
            None => continue,
        };
        while !remainder.is_empty() {
            let limit = if remainder.len() > bytes {
                memchr(b'\n', &remainder[bytes..]).map_or(remainder.len(), |end| bytes + end + 1)
            } else {
                remainder.len()
            };
            let (chunk, rest) = remainder.split_at(limit);
            chunks.push(chunk);
            remainder = rest;
        }
    }
    chunks
}

pub type Shards<T> = Vec<Vec<T>>;

pub fn buckets<T>() -> Shards<T> {
    (0..PARTITIONS).map(|_| Vec::new()).collect()
}

pub fn transpose<T>(parts: Vec<Shards<T>>) -> Vec<Shards<T>> {
    let mut grouped = buckets();
    for part in parts {
        for (position, items) in part.into_iter().enumerate() {
            if !items.is_empty() {
                grouped[position].push(items);
            }
        }
    }
    grouped
}

pub fn route<'a, T: Send>(
    files: &'a [Mmap],
    workers: usize,
    parse: impl Fn(&'a [u8]) -> Option<(usize, T)> + Sync,
) -> Vec<Shards<T>> {
    let chunks = chunks(files);
    let groups = (workers * 4).min(chunks.len()).max(1);
    // Interleave chunks to spread long and short files across workers.
    let parts = (0..groups)
        .into_par_iter()
        .map(|index| {
            let mut shards = buckets();
            for chunk in chunks.iter().skip(index).step_by(groups) {
                let mut start = 0;
                for end in memchr::memchr_iter(b'\n', chunk).chain(std::iter::once(chunk.len())) {
                    let line = &chunk[start..end];
                    start = end + 1;
                    if let Some((partition, item)) = parse(line) {
                        shards[partition].push(item);
                    }
                }
            }
            shards
        })
        .collect();
    transpose(parts)
}

pub fn merge<T>(mut parts: Shards<T>) -> Vec<T> {
    let capacity = parts.iter().map(Vec::len).sum::<usize>();
    let mut result = parts.pop().unwrap_or_default();
    result.reserve_exact(capacity - result.len());
    for mut part in parts {
        result.append(&mut part);
    }
    result
}

use std::sync::atomic::{
    AtomicU64,
    Ordering::{AcqRel, Acquire, Relaxed},
};

struct Vertex {
    key: AtomicU64,
    parent: AtomicU64,
}

pub struct Aliases {
    hash: RandomState,
    vertices: Vec<Vertex>,
}

impl Aliases {
    fn slot(&self, key: Key, insert: bool) -> Option<usize> {
        let mask = self.vertices.len() - 1;
        let mut i = self.hash.hash_one(key) as usize & mask;
        loop {
            let cell = &self.vertices[i].key;
            let mut found = cell.load(Relaxed);
            if found == 0 {
                if !insert {
                    return None;
                }
                found = match cell.compare_exchange(0, key, Relaxed, Relaxed) {
                    Ok(_) => key,
                    Err(current) => current,
                };
            }
            if found == key {
                return Some(i);
            }
            i = (i + 1) & mask;
        }
    }

    fn root(&self, mut i: usize) -> usize {
        loop {
            let parent = self.vertices[i].parent.load(Acquire) as usize;
            if parent == i {
                return i;
            }
            let grandparent = self.vertices[parent].parent.load(Acquire);
            let _ = self.vertices[i].parent.compare_exchange(
                parent as u64,
                grandparent,
                AcqRel,
                Acquire,
            );
            i = grandparent as usize;
        }
    }

    fn join(&self, mut a: usize, mut b: usize, names: &Names<'_>) {
        loop {
            a = self.root(a);
            b = self.root(b);
            if a == b {
                return;
            }
            // Every successful link points toward a strictly smaller name.
            // A competing link retries at the new root, so cycles cannot form.
            if names.less(
                self.vertices[a].key.load(Relaxed),
                self.vertices[b].key.load(Relaxed),
            ) {
                std::mem::swap(&mut a, &mut b);
            }
            if self.vertices[a]
                .parent
                .compare_exchange(a as u64, b as u64, AcqRel, Acquire)
                .is_ok()
            {
                return;
            }
        }
    }

    pub fn resolve(&self, key: Key) -> Key {
        self.slot(key, false)
            .map_or(key, |i| self.vertices[i].parent.load(Relaxed))
    }
}

pub fn aliases<'a>(files: &'a [Mmap], names: &Names<'a>) -> Aliases {
    let chunks = sized_chunks(files, 128 * 1024);
    // Each row introduces at most two names. Include unterminated last
    // lines and retain at least one empty slot for unsuccessful lookups.
    let lines: usize = chunks
        .par_iter()
        .map(|c| memchr::memchr_iter(b'\n', c).count() + 1)
        .sum();
    let capacity = (2 * lines + 1).next_power_of_two();
    let graph = Aliases {
        hash: RandomState::new(),
        vertices: (0..capacity)
            .into_par_iter()
            .map(|i| Vertex {
                key: AtomicU64::new(0),
                parent: AtomicU64::new(i as u64),
            })
            .collect(),
    };
    chunks.par_iter().for_each(|chunk| {
        for [a, b] in chunk.split(|&byte| byte == b'\n').filter_map(fields::<2>) {
            if !a.is_empty() && !b.is_empty() {
                let a = graph.slot(names.key(a), true).unwrap();
                let b = graph.slot(names.key(b), true).unwrap();
                graph.join(a, b, names);
            }
        }
    });
    graph.vertices.par_iter().enumerate().for_each(|(i, v)| {
        v.parent.store(graph.root(i) as u64, Relaxed);
    });
    // The first pass finishes every path before this pass replaces root
    // indices with keys. Lookups then need just one probe sequence.
    graph.vertices.par_iter().for_each(|v| {
        let person = graph.vertices[v.parent.load(Relaxed) as usize]
            .key
            .load(Relaxed);
        v.parent.store(person, Relaxed);
    });
    graph
}
