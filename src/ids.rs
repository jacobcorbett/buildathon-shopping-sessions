use crate::input::Id;
use ahash::{AHashMap, RandomState};
use std::sync::{Arc, Mutex};

pub type Key = u64;
const INTERNED: Key = 1 << 63;
const LONG_ASCII_MIN: Key = 2 << 56;
const ASCII_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;

#[derive(Default)]
struct Interned {
    keys: AHashMap<Arc<[u8]>, Key>,
    values: Vec<Arc<[u8]>>,
}

#[derive(Default)]
pub struct Names {
    interned: [Mutex<Interned>; 32],
    hash: RandomState,
}

#[inline]
fn pack(first_eight: u64) -> u64 {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // This branch is only compiled for targets with BMI2.
        unsafe { core::arch::x86_64::_pext_u64(first_eight, ASCII_BITS) }
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        first_eight
            .to_be_bytes()
            .into_iter()
            .fold(0, |key, byte| (key << 7) | u64::from(byte))
    }
}

fn unpack(packed: u64) -> [u8; 8] {
    #[cfg(all(target_arch = "x86_64", target_feature = "bmi2"))]
    {
        // This branch is only compiled for targets with BMI2.
        unsafe { core::arch::x86_64::_pdep_u64(packed, ASCII_BITS) }.to_be_bytes()
    }
    #[cfg(not(all(target_arch = "x86_64", target_feature = "bmi2")))]
    {
        std::array::from_fn(|index| ((packed >> (7 * (7 - index))) & 0x7f) as u8)
    }
}

impl Names {
    #[inline]
    pub fn key(&self, id: Id<'_>) -> Key {
        // Nine ASCII bytes fit in 63 bits. Reserve the two lowest
        // possible leading bytes for shorter, length-marked ASCII keys.
        if let Ok(short) = <&[u8; 9]>::try_from(id) {
            let first_eight = u64::from_be_bytes(short[..8].try_into().unwrap());
            let last = short[8];
            if short[0] >= 2 && first_eight & !ASCII_BITS == 0 && last < 128 {
                return (pack(first_eight) << 7) | u64::from(last);
            }
        } else if id.len() <= 8 {
            let mut padded = [0; 8];
            padded[8 - id.len()..].copy_from_slice(id);
            let bits = u64::from_be_bytes(padded);
            if bits & !ASCII_BITS == 0 {
                return (1 << (7 * id.len())) | pack(bits);
            }
        }
        self.intern(id)
    }

    #[cold]
    fn intern(&self, id: Id<'_>) -> Key {
        let shard = self.hash.hash_one(id) as usize % self.interned.len();
        let mut state = self.interned[shard].lock().unwrap();
        if let Some(&key) = state.keys.get(id) {
            return key;
        }
        let key =
            INTERNED | (state.values.len() as u64 * self.interned.len() as u64) | shard as u64;
        let id = Arc::<[u8]>::from(id);
        state.values.push(id.clone());
        state.keys.insert(id, key);
        key
    }

    pub fn compare(&self, a: Key, b: Key) -> std::cmp::Ordering {
        if a == b {
            return std::cmp::Ordering::Equal;
        }
        if (LONG_ASCII_MIN..INTERNED).contains(&a) && (LONG_ASCII_MIN..INTERNED).contains(&b) {
            a.cmp(&b)
        } else if a < INTERNED && b < INTERNED {
            // Right-pad to nine bytes to compare the common prefix. Length
            // breaks ties when an identifier ends in zero bytes.
            let order = |key: Key| {
                if key >= LONG_ASCII_MIN {
                    (key, 9)
                } else {
                    let len = key.ilog2() / 7;
                    ((key ^ (1 << (7 * len))) << (7 * (9 - len)), len)
                }
            };
            order(a).cmp(&order(b))
        } else {
            self.bytes(a).cmp(&self.bytes(b))
        }
    }

    pub fn with_bytes<T>(&self, key: Key, consume: impl FnOnce(&[u8]) -> T) -> T {
        if key & INTERNED != 0 {
            let id = (key & !INTERNED) as usize;
            let state = self.interned[id % self.interned.len()].lock().unwrap();
            return consume(&state.values[id / self.interned.len()]);
        }
        if key < LONG_ASCII_MIN {
            let length = key.ilog2() as usize / 7;
            let packed = key ^ (1 << (length * 7));
            return consume(&unpack(packed)[8 - length..]);
        }
        let mut id = [0; 9];
        id[..8].copy_from_slice(&unpack(key >> 7));
        id[8] = (key & 0x7f) as u8;
        consume(&id)
    }
    pub fn bytes(&self, key: Key) -> Vec<u8> {
        self.with_bytes(key, |id| id.to_vec())
    }
}
