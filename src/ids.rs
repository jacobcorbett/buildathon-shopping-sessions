use crate::input::Id;
use ahash::AHashMap;
use std::sync::Mutex;

pub type Key = u64;
const INTERNED: Key = 1 << 63;
const LONG_ASCII_MIN: Key = 2 << 56;
const ASCII_BITS: u64 = 0x7f7f_7f7f_7f7f_7f7f;

#[derive(Default)]
struct Interned<'a> {
    keys: AHashMap<Id<'a>, Key>,
    values: Vec<Id<'a>>,
}

#[derive(Default)]
pub struct Names<'a> {
    interned: Mutex<Interned<'a>>,
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

impl<'a> Names<'a> {
    #[inline]
    pub fn key(&self, id: Id<'a>) -> Key {
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
    fn intern(&self, id: Id<'a>) -> Key {
        let mut state = self.interned.lock().unwrap();
        if let Some(&key) = state.keys.get(id) {
            return key;
        }
        let key = INTERNED | state.values.len() as u64;
        state.values.push(id);
        state.keys.insert(id, key);
        key
    }

    pub fn bytes(&self, key: Key) -> Vec<u8> {
        if key & INTERNED != 0 {
            return self.interned.lock().unwrap().values[(key & !INTERNED) as usize].to_vec();
        }
        if key < LONG_ASCII_MIN {
            let length = key.ilog2() as usize / 7;
            let packed = key ^ (1 << (length * 7));
            return unpack(packed)[8 - length..].to_vec();
        }
        let mut id = [0; 9];
        id[..8].copy_from_slice(&unpack(key >> 7));
        id[8] = (key & 0x7f) as u8;
        id.to_vec()
    }
}
