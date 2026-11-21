use crate::ids::{Key, Names};
use ahash::{AHashMap, RandomState};
use memchr::memchr;
use memmap2::{Mmap, MmapOptions};
use rayon::prelude::*;
use std::sync::{
    Mutex,
    atomic::{AtomicU64, Ordering::Relaxed},
};
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

#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
fn event_fields(line: &[u8]) -> Option<[&[u8]; 7]> {
    use core::arch::x86_64::*;
    let len = line.len();
    if !(65..=128).contains(&len) {
        return fields(line);
    }
    #[inline(always)]
    unsafe fn masks(p: *const u8) -> (u64, u64) {
        unsafe {
            let a = _mm256_loadu_si256(p.cast());
            let b = _mm256_loadu_si256(p.add(32).cast());
            let mask = |c| {
                let k = _mm256_set1_epi8(c);
                let lo = _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, k)) as u32;
                let hi = _mm256_movemask_epi8(_mm256_cmpeq_epi8(b, k)) as u32;
                u64::from(lo) | (u64::from(hi) << 32)
            };
            (mask(b',' as i8), mask(b'"' as i8))
        }
    }
    let ((mut left, ql), (right, qr)) =
        unsafe { (masks(line.as_ptr()), masks(line.as_ptr().add(len - 64))) };
    let overlap = 128 - len;
    if left.count_ones() + (right >> overlap).count_ones() != 6 {
        return None;
    }
    if left.count_ones() < 3 || right.count_ones() < 3 {
        return fields(line);
    }
    let mut right = right.reverse_bits();
    let mut ends = [0; 7];
    ends[6] = len;
    for i in 0..3 {
        ends[i] = left.trailing_zeros() as usize;
        left &= left - 1;
        ends[5 - i] = len - 1 - right.trailing_zeros() as usize;
        right &= right - 1;
    }
    let mut row = [&[][..]; 7];
    let mut start = 0usize;
    let mut stripped = 0;
    for (field, end) in row.iter_mut().zip(ends) {
        let open = line[start.min(len - 1)] == b'"';
        let close = line[end.saturating_sub(1)] == b'"';
        let trim = usize::from((end - start >= 2) & open & close);
        *field = &line[start + trim..end - trim];
        start = end + 1;
        stripped += 2 * trim as u32;
    }
    (stripped == ql.count_ones() + (qr >> overlap).count_ones()).then_some(row)
}
#[inline(always)]
pub fn event_row(line: &[u8]) -> Option<[&[u8]; 7]> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    {
        event_fields(line)
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "avx2")))]
    {
        fields(line)
    }
}

#[inline(always)]
pub fn fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if (32..=64).contains(&line.len()) {
        return short_fields(line);
    }
    #[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
    if (65..=128).contains(&line.len()) {
        return masked_fields(line);
    }
    scalar_fields(line)
}

#[inline(never)]
fn scalar_fields<const N: usize>(mut line: &[u8]) -> Option<[&[u8]; N]> {
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
#[inline(always)]
fn short_fields<const N: usize>(line: &[u8]) -> Option<[&[u8]; N]> {
    use core::arch::x86_64::*;
    let len = line.len();
    let offset = len - 32;
    let (mut commas, quotes) = unsafe {
        let a = _mm256_loadu_si256(line.as_ptr().cast());
        let b = _mm256_loadu_si256(line.as_ptr().add(offset).cast());
        let mask = |c| {
            let c = _mm256_set1_epi8(c);
            _mm256_movemask_epi8(_mm256_cmpeq_epi8(a, c)) as u32 as u64
                | ((_mm256_movemask_epi8(_mm256_cmpeq_epi8(b, c)) as u32 as u64) << offset)
        };
        (mask(b',' as i8), mask(b'"' as i8).count_ones())
    };
    if commas.count_ones() as usize != N - 1 {
        return None;
    }
    let mut row = [&[][..]; N];
    let mut start = 0usize;
    let mut stripped = 0;
    for (i, field) in row.iter_mut().enumerate() {
        let end = if i == N - 1 {
            len
        } else {
            commas.trailing_zeros() as usize
        };
        commas &= commas.wrapping_sub(1);
        let open = line[start.min(len - 1)] == b'"';
        let close = line[end.saturating_sub(1)] == b'"';
        let trim = usize::from((end - start >= 2) & open & close);
        *field = &line[start + trim..end - trim];
        start = end + 1;
        stripped += 2 * trim as u32;
    }
    (stripped == quotes).then_some(row)
}

// Read overlapping vectors at the tail so every load stays within the line.
#[cfg(all(target_arch = "x86_64", target_feature = "avx2"))]
#[inline(always)]
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
    if bytes.len() == 13 {
        let digits = u64::from_le_bytes(bytes[5..].try_into().unwrap()) ^ 0x3030_3030_3030_3030;
        if digits & 0xf0f0_f0f0_f0f0_f0f0 != 0
            || (digits + 0x0606_0606_0606_0606) & 0x1010_1010_1010_1010 != 0
        {
            return None;
        }
        let pairs = digits.wrapping_mul(10).wrapping_add(digits >> 8) & 0x00ff_00ff_00ff_00ff;
        let quads = pairs.wrapping_mul(100).wrapping_add(pairs >> 16) & 0x0000_ffff_0000_ffff;
        let mut head = 0;
        for &byte in &bytes[..5] {
            head = head * 10 + u64::from(digit(byte)?);
        }
        return Some(head * 100_000_000 + (quads & 65535) * 10_000 + (quads >> 32));
    }
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

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
static MONTHS: [[i32; 13]; 10000] = {
    let mut years = [[0; 13]; 10000];
    let mut base = -719528;
    let mut year = 0;
    while year < 10000 {
        let leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
        let lengths = [31, 28 + leap as i32, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
        let mut m = 0;
        while m < 12 {
            years[year][m] = base;
            base += lengths[m];
            m += 1;
        }
        years[year][12] = base;
        year += 1;
    }
    years
};

#[cfg(all(target_arch = "x86_64", target_feature = "sse4.1"))]
#[inline(always)]
fn ascii_calendar(bytes: &[u8]) -> Option<i64> {
    use core::arch::x86_64::*;
    // The caller has established a full 20- or 25-byte timestamp.
    unsafe {
        let a = _mm_loadu_si128(bytes.as_ptr().cast());
        let b = _mm_loadu_si128(bytes.as_ptr().add(3).cast());
        let marks = _mm_setr_epi8(
            0, 0, 0, 0, b'-' as i8, 0, 0, b'-' as i8, 0, 0, b'T' as i8, 0, 0, b':' as i8, 0, 0,
        );
        let select = _mm_setr_epi8(0, 0, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0, -1, 0, 0);
        if _mm_testz_si128(_mm_xor_si128(a, marks), select) == 0 || bytes[16] != b':' {
            return None;
        }
        let a = _mm_sub_epi8(a, _mm_set1_epi8(b'0' as i8));
        let b = _mm_sub_epi8(b, _mm_set1_epi8(b'0' as i8));
        let leading = _mm_shuffle_epi8(
            a,
            _mm_setr_epi8(0, 1, 2, 3, 5, 6, 8, 9, 11, 12, 14, 15, -1, -1, -1, -1),
        );
        let seconds = _mm_shuffle_epi8(
            b,
            _mm_setr_epi8(
                -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1, 14, 15, -1, -1,
            ),
        );
        let digits = _mm_or_si128(leading, seconds);
        let invalid = _mm_subs_epu8(digits, _mm_set1_epi8(9));
        let pairs = _mm_maddubs_epi16(digits, _mm_set1_epi16(0x010a));
        let limits = _mm_setr_epi16(99, 99, 12, 31, 23, 59, 59, 0);
        let bad = _mm_or_si128(invalid, _mm_cmpgt_epi16(pairs, limits));
        if _mm_testz_si128(bad, bad) == 0 {
            return None;
        }
        let scaled = _mm_madd_epi16(pairs, _mm_setr_epi16(100, 1, 0, 0, 3600, 60, 0, 0));
        let year = _mm_cvtsi128_si32(scaled) as usize;
        let month = _mm_extract_epi16::<2>(pairs) as usize;
        let day = _mm_extract_epi16::<3>(pairs);
        if month == 0 || day == 0 {
            return None;
        }
        let year = &MONTHS[year];
        let begin = year[month - 1];
        if day > year[month] - begin {
            return None;
        }
        let seconds = _mm_extract_epi32::<2>(scaled) + _mm_extract_epi16::<6>(pairs);
        Some(i64::from(begin + day - 1) * DAY + i64::from(seconds) * 1000)
    }
}
#[cfg(not(all(target_arch = "x86_64", target_feature = "sse4.1")))]
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

#[inline(always)]
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
    prepare: impl Fn(&mut [(usize, T)]) + Sync,
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
            let mut batch = Vec::with_capacity(64);
            let mut flush = |batch: &mut Vec<(usize, T)>| {
                prepare(batch);
                for (partition, item) in batch.drain(..) {
                    shards[partition].push(item);
                }
            };
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
                        batch.push((partition, item));
                        if batch.len() == 64 {
                            flush(&mut batch);
                        }
                    }
                }
            }
            flush(&mut batch);
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

#[derive(Default)]
struct Extra {
    ids: AHashMap<Key, usize>,
    keys: Vec<Key>,
}

// Each occupied slot is a stable person number. Probe chains are bounded;
// exact overflow dictionaries also handle inputs with many distinct users.
pub struct People {
    slots: Vec<AtomicU64>,
    hash: RandomState,
    overflow: [Mutex<Extra>; 64],
}

impl People {
    pub fn new(bytes: usize) -> Self {
        let size = (bytes / 920 + 1).next_power_of_two();
        Self {
            slots: (0..size).map(|_| AtomicU64::new(0)).collect(),
            hash: RandomState::new(),
            overflow: std::array::from_fn(|_| Mutex::default()),
        }
    }
    #[inline(always)]
    pub fn person(&self, key: Key) -> Key {
        self.person_hinted(key, self.hash.hash_one(key) as usize)
    }
    #[inline(always)]
    pub fn person_hinted(&self, key: Key, hash: usize) -> Key {
        let first = hash & (self.slots.len() - 1);
        if self.slots[first].load(Relaxed) == key {
            return first as Key;
        }
        self.insert_or_probe(key, hash)
    }
    #[inline(never)]
    fn insert_or_probe(&self, key: Key, hash: usize) -> Key {
        let mask = self.slots.len() - 1;
        for offset in 0..self.slots.len().min(16) {
            let i = hash.wrapping_add(offset) & mask;
            let mut stored = self.slots[i].load(Relaxed);
            if stored == 0 {
                stored = self.slots[i]
                    .compare_exchange(0, key, Relaxed, Relaxed)
                    .unwrap_or_else(|v| v);
                if stored == 0 {
                    return i as Key;
                }
            }
            if stored == key {
                return i as Key;
            }
        }
        self.extra(key, hash)
    }
    #[cold]
    fn extra(&self, key: Key, hash: usize) -> Key {
        let bucket = hash % 64;
        let mut extra = self.overflow[bucket].lock().unwrap();
        let extra = &mut *extra;
        let index = *extra.ids.entry(key).or_insert_with(|| {
            let index = extra.keys.len();
            extra.keys.push(key);
            index
        });
        (self.slots.len() + index * 64 + bucket) as Key
    }
    #[inline]
    pub fn hint(&self, key: Key) -> usize {
        let mask = self.slots.len() - 1;
        let hash = self.hash.hash_one(key) as usize & (mask | 63);
        prefetch(&self.slots[hash & mask]);
        hash
    }
    pub fn key(&self, person: Key) -> Key {
        let id = person as usize;
        if id < self.slots.len() {
            self.slots[id].load(Relaxed)
        } else {
            let i = id - self.slots.len();
            self.overflow[i % 64].lock().unwrap().keys[i / 64]
        }
    }
    pub fn direct_len(&self) -> usize {
        self.slots.len()
    }
}

pub struct Aliases {
    parent: Vec<usize>,
}
impl Aliases {
    #[inline]
    pub fn person(&self, id: Key) -> Key {
        self.parent.get(id as usize).copied().unwrap_or(id as usize) as Key
    }
    pub fn prefetch(&self, id: Key) {
        if let Some(slot) = self.parent.get(id as usize) {
            prefetch(slot);
        }
    }
}

#[inline(always)]
fn link_fields(line: &[u8]) -> Option<[&[u8]; 2]> {
    #[cfg(target_arch = "x86_64")]
    if (16..=32).contains(&line.len()) {
        use core::arch::x86_64::*;
        let off = line.len() - 16;
        let (comma, quotes) = unsafe {
            let a = _mm_loadu_si128(line.as_ptr().cast());
            let b = _mm_loadu_si128(line.as_ptr().add(off).cast());
            let mask = |k| {
                let k = _mm_set1_epi8(k);
                _mm_movemask_epi8(_mm_cmpeq_epi8(a, k)) as u32
                    | ((_mm_movemask_epi8(_mm_cmpeq_epi8(b, k)) as u32) << off)
            };
            (mask(b',' as i8), mask(b'"' as i8).count_ones())
        };
        if !comma.is_power_of_two() {
            return None;
        }
        let sep = comma.trailing_zeros() as usize;
        let (a, b) = (&line[..sep], &line[sep + 1..]);
        let qa = usize::from(a.len() >= 2 && a[0] == b'"' && a[a.len() - 1] == b'"');
        let qb = usize::from(b.len() >= 2 && b[0] == b'"' && b[b.len() - 1] == b'"');
        return (quotes == 2 * (qa + qb) as u32)
            .then(|| [&a[qa..a.len() - qa], &b[qb..b.len() - qb]]);
    }
    fields(line)
}

pub fn aliases(files: &[Mmap], names: &Names, people: &People) -> Aliases {
    fn root(parent: &mut [usize], mut i: usize) -> usize {
        while i != parent[i] {
            parent[i] = parent[parent[i]];
            i = parent[i];
        }
        i
    }
    let mut parent: Vec<_> = (0..people.direct_len()).collect();
    let mut pending = Vec::with_capacity(64);
    let mut join = |pending: &mut Vec<(Key, Key, Key, Key)>| {
        for &(a, b, _, _) in &*pending {
            people.hint(a);
            people.hint(b);
        }
        for (a, b, _, _) in &mut *pending {
            *a = people.person(*a);
            *b = people.person(*b);
            if let Some(p) = parent.get(*a as usize) {
                prefetch(p);
            }
            if let Some(p) = parent.get(*b as usize) {
                prefetch(p);
            }
        }
        for (a, b, left, right) in pending.drain(..) {
            let a = a as usize;
            let b = b as usize;
            let need = a.max(b) + 1;
            if need > parent.len() {
                parent.extend(parent.len()..need);
            }
            let left_slot = a;
            let right_slot = b;
            let a = root(&mut parent, a);
            let b = root(&mut parent, b);
            if a != b {
                if names
                    .compare(
                        if a == left_slot {
                            left
                        } else {
                            people.key(a as Key)
                        },
                        if b == right_slot {
                            right
                        } else {
                            people.key(b as Key)
                        },
                    )
                    .is_lt()
                {
                    parent[b] = a;
                } else {
                    parent[a] = b;
                }
            }
        }
    };
    for file in files {
        let mut lines = memchr::memchr_iter(b'\n', file);
        let Some(header) = lines.next() else {
            continue;
        };
        let mut start = header + 1;
        for end in lines.chain(std::iter::once(file.len())) {
            let Some(row) = file.get(start..end) else {
                break;
            };
            start = end + 1;
            if let Some([a, b]) = link_fields(row) {
                if !a.is_empty() && !b.is_empty() {
                    let a = names.key(a);
                    let b = names.key(b);
                    pending.push((a, b, a, b));
                    if pending.len() == 64 {
                        join(&mut pending);
                    }
                }
            }
        }
    }
    join(&mut pending);
    for i in 0..parent.len() {
        parent[i] = root(&mut parent, i);
    }
    Aliases { parent }
}
