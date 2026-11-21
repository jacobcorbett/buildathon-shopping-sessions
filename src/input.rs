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
    if (32..=128).contains(&line.len()) {
        return masked_fields(line);
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

// Read overlapping vectors at the tail so every load stays within the line.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline]
fn masked_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use core::arch::x86_64::*;
    let mut commas = 0u128;
    let mut quote_mask = 0u128;
    let mut offset = 0;
    loop {
        // Caller guarantees at least 32 bytes and offset <= len - 32.
        unsafe {
            let block = _mm256_loadu_si256(line.as_ptr().add(offset).cast());
            let mask =
                |c| _mm256_movemask_epi8(_mm256_cmpeq_epi8(block, _mm256_set1_epi8(c))) as u32;
            commas |= u128::from(mask(b',' as i8)) << offset;
            quote_mask |= u128::from(mask(b'"' as i8)) << offset;
        }
        if offset + 32 == line.len() {
            break;
        }
        offset = (offset + 32).min(line.len() - 32);
    }
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    let mut result = [&[][..]; N];
    let mut stripped = 0;
    let mut start = 0;
    for field in &mut result {
        let end = if commas == 0 {
            line.len()
        } else {
            commas.trailing_zeros() as usize
        };
        commas &= commas.wrapping_sub(1);
        let raw = &line[start..end];
        let quoted = usize::from(raw.len() >= 2 && raw[0] == b'"' && raw[raw.len() - 1] == b'"');
        stripped += 2 * quoted;
        *field = &raw[quoted..raw.len() - quoted];
        start = end + 1;
    }
    (quote_mask.count_ones() as usize == stripped).then_some(result)
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
    #[cfg(all(target_arch = "x86_64", target_feature = "ssse3"))]
    let [year, month, day, hour, minute, second] = {
        use core::arch::x86_64::*;
        // Read the final 16 bytes of YYYY-MM-DDTHH:MM:SS. Shuffle the
        // decimal pairs together, validate their digits, then multiply-add.
        unsafe {
            let digits = _mm_sub_epi8(
                _mm_loadu_si128(bytes.as_ptr().add(3).cast()),
                _mm_set1_epi8(b'0' as i8),
            );
            let order = _mm_setr_epi8(2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1, -1, -1);
            let digits = _mm_shuffle_epi8(digits, order);
            if _mm_movemask_epi8(_mm_cmpeq_epi8(
                _mm_min_epu8(digits, _mm_set1_epi8(9)),
                digits,
            )) != 65535
            {
                return None;
            }
            let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
            [
                pair(bytes, 0)? * 100 + pair(bytes, 2)?,
                _mm_extract_epi16::<0>(pairs) as u32,
                _mm_extract_epi16::<1>(pairs) as u32,
                _mm_extract_epi16::<2>(pairs) as u32,
                _mm_extract_epi16::<3>(pairs) as u32,
                _mm_extract_epi16::<4>(pairs) as u32,
            ]
        }
    };
    #[cfg(not(all(target_arch = "x86_64", target_feature = "ssse3")))]
    let [year, month, day, hour, minute, second] = [
        pair(bytes, 0)? * 100 + pair(bytes, 2)?,
        pair(bytes, 5)?,
        pair(bytes, 8)?,
        pair(bytes, 11)?,
        pair(bytes, 14)?,
        pair(bytes, 17)?,
    ];
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

pub fn buckets<T>(count: usize) -> Shards<T> {
    (0..count).map(|_| Vec::new()).collect()
}

pub fn transpose<T>(parts: Vec<Shards<T>>, count: usize) -> Vec<Shards<T>> {
    let mut grouped = buckets(count);
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
    bytes_per_row: usize,
    partitions: usize,
    parse: impl Fn(&'a [u8]) -> Option<(usize, T)> + Sync,
) -> Vec<Shards<T>> {
    let chunks = chunks(files);
    let groups = (workers * 4).min(chunks.len()).max(1);
    // Interleave chunks to spread long and short files across workers.
    let parts = (0..groups)
        .into_par_iter()
        .map(|index| {
            let assigned = chunks.iter().skip(index).step_by(groups);
            let bytes: usize = assigned.clone().map(|chunk| chunk.len()).sum();
            let mut shards: Shards<T> = (0..partitions)
                .map(|_| Vec::with_capacity(bytes / bytes_per_row / partitions))
                .collect();
            for chunk in assigned {
                for line in memchr::memchr_iter(b'\n', chunk)
                    .chain(std::iter::once(chunk.len()))
                    .scan(0, |start, end| {
                        let line = chunk.get(*start..end)?;
                        *start = end + 1;
                        Some(line)
                    })
                {
                    if let Some((partition, item)) = parse(line) {
                        shards[partition].push(item);
                    }
                }
            }
            shards
        })
        .collect();
    transpose(parts, partitions)
}

#[inline]
pub fn prefetch<T>(item: &T) {
    #[cfg(target_arch = "x86_64")]
    unsafe {
        core::arch::x86_64::_mm_prefetch(
            (item as *const T).cast(),
            core::arch::x86_64::_MM_HINT_T0,
        );
    }
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

// During union, values are parent slots. The last pass replaces each slot
// with its canonical key for a single lookup per event or click.
#[derive(Clone, Copy, Default)]
struct AliasSlot {
    key: Key,
    value: Key,
}

pub struct Aliases {
    slots: Vec<AliasSlot>,
    hash: RandomState,
}

impl Aliases {
    #[inline]
    fn slot(&self, key: Key) -> usize {
        let mask = self.slots.len() - 1;
        let mut i = self.hash.hash_one(key) as usize & mask;
        while self.slots[i].key != 0 && self.slots[i].key != key {
            i = (i + 1) & mask;
        }
        i
    }

    #[inline]
    pub fn prefetch(&self, key: Key) {
        let slot = self.hash.hash_one(key) as usize & (self.slots.len() - 1);
        prefetch(&self.slots[slot]);
    }

    #[inline]
    pub fn person(&self, key: Key) -> Key {
        let slot = self.slot(key);
        if self.slots[slot].key == 0 {
            key
        } else {
            self.slots[slot].value
        }
    }

    fn root(&mut self, key: Key) -> usize {
        let mut i = self.slot(key);
        if self.slots[i].key == 0 {
            self.slots[i].key = key;
            self.slots[i].value = i as Key;
        }
        while self.slots[i].value != i as Key {
            self.slots[i].value = self.slots[self.slots[i].value as usize].value;
            i = self.slots[i].value as usize;
        }
        i
    }
}

pub fn aliases<'a>(files: &'a [Mmap], names: &Names<'a>) -> Aliases {
    let rows: usize = files
        .iter()
        .map(|file| memchr::memchr_iter(b'\n', file).count())
        .sum();
    // Including headers covers a final row without LF. At most two new keys
    // can occur per row, leaving an empty slot even for disjoint links.
    let capacity = (rows * 2 + 1).next_power_of_two();
    let mut map = Aliases {
        slots: vec![AliasSlot::default(); capacity],
        hash: RandomState::new(),
    };
    for [left, right] in files
        .iter()
        .flat_map(|file| lines(file).filter_map(fields::<2>))
    {
        if left.is_empty() || right.is_empty() {
            continue;
        }
        let a = map.root(names.key(left));
        let b = map.root(names.key(right));
        if a != b {
            if names.less(map.slots[a].key, map.slots[b].key) {
                map.slots[b].value = a as Key;
            } else {
                map.slots[a].value = b as Key;
            }
        }
    }
    for slot in 0..capacity {
        if map.slots[slot].key != 0 {
            let mut root = map.slots[slot].value as usize;
            while map.slots[root].value != root as Key {
                root = map.slots[root].value as usize;
            }
            map.slots[slot].value = root as Key;
        }
    }
    for i in 0..capacity {
        let root = map.slots[i].value as usize;
        map.slots[i].value = map.slots[root].key;
    }
    map
}
