//! Work-in-progress: implement [`Channel`]
#![allow(dead_code)]

use std::hash::{BuildHasher, Hash, Hasher};
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use crate::wait_queue::WaitQueue;
use crate::HashMap;

/// Work-in-progress: [`Receiver`].
pub struct Receiver<T: 'static + Sync> {
    _channel: Arc<Channel<T>>,
}

/// Work-in-progress: [`Sender`].
pub struct Sender<T: 'static + Sync> {
    _channel: Arc<Channel<T>>,
}

#[derive(Eq, Hash, PartialEq)]
struct Key(usize);

struct KeyHasher(u64);

impl BuildHasher for KeyHasher {
    type Hasher = Self;

    #[inline]
    fn build_hasher(&self) -> Self::Hasher {
        KeyHasher(0)
    }
}

impl Hasher for KeyHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, _bytes: &[u8]) {
        unreachable!();
    }

    #[inline]
    fn write_usize(&mut self, i: usize) {
        // `https://mostlymangling.blogspot.com/2019/01/better-stronger-mixer-and-test-procedure.html`.
        self.0 = i as u64;
        self.0 = (self.0 ^ (((self.0 >> 25) | (self.0 << 39)) ^ ((self.0 >> 50) | (self.0 << 14))))
            * 0xA24B_AED4_963E_E407_u64;
        self.0 = (self.0 ^ (((self.0 >> 24) | (self.0 << 40)) ^ ((self.0 >> 49) | (self.0 << 15))))
            * 0x9FB2_1C65_1E98_DF25_u64;
        self.0 ^= self.0 >> 28;
    }
}

/// Work-in-progress: [`Channel`].
struct Channel<T: 'static + Sync> {
    _messages: HashMap<Key, T, KeyHasher>,
    _produced: AtomicUsize,
    _consumed: AtomicUsize,
    _num_receivers: AtomicUsize,
    _num_senders: AtomicUsize,
    _wait_queue: WaitQueue,
}
