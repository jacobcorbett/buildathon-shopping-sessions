use crate::ids::{Key, Names};
use ahash::AHashMap;
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
    if bytes[7] != b'-' || bytes[10] != b'T' || bytes[13] != b':' || bytes[16] != b':' {
        return None;
    }
    let year = pair(bytes, 0)? * 100 + pair(bytes, 2)?;
    let month = pair(bytes, 5)?;
    let day = pair(bytes, 8)?;
    let hour = pair(bytes, 11)?;
    let minute = pair(bytes, 14)?;
    let second = pair(bytes, 17)?;
    if !(1..=12).contains(&month) || day == 0 || hour > 23 || minute > 59 || second > 59 {
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
    Some(days * DAY + i64::from((hour * 60 + minute) * 60 + second) * 1000)
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
    let mut chunks = Vec::new();
    for file in files {
        let mut remainder = match memchr(b'\n', file) {
            Some(header_end) => &file[header_end + 1..],
            None => continue,
        };
        while !remainder.is_empty() {
            let limit = if remainder.len() > CHUNK_BYTES {
                memchr(b'\n', &remainder[CHUNK_BYTES..])
                    .map_or(remainder.len(), |end| CHUNK_BYTES + end + 1)
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

pub fn aliases<'a>(files: &'a [Mmap], names: &Names<'a>) -> AHashMap<Key, Key> {
    fn root(parent: &mut [usize], mut node: usize) -> usize {
        while parent[node] != node {
            parent[node] = parent[parent[node]];
            node = parent[node];
        }
        node
    }

    let mut ids = AHashMap::new();
    let mut parent = Vec::new();
    let mut sizes = Vec::new();
    let mut minimum = Vec::new();
    for [left, right] in files
        .iter()
        .flat_map(|file| lines(file).filter_map(fields::<2>))
    {
        if left.is_empty() || right.is_empty() {
            continue;
        }
        let mut index = |id| {
            let key = names.key(id);
            *ids.entry(key).or_insert_with(|| {
                let n = parent.len();
                parent.push(n);
                sizes.push(1);
                minimum.push((id, key));
                n
            })
        };
        let (left, right) = (index(left), index(right));
        let (mut a, mut b) = (root(&mut parent, left), root(&mut parent, right));
        if a == b {
            continue;
        }
        if sizes[a] < sizes[b] {
            std::mem::swap(&mut a, &mut b);
        }
        parent[b] = a;
        sizes[a] += sizes[b];
        minimum[a] = minimum[a].min(minimum[b]);
    }
    ids.into_iter()
        .filter_map(|(key, node)| {
            let person = minimum[root(&mut parent, node)].1;
            (key != person).then_some((key, person))
        })
        .collect()
}
