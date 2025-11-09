use std::alloc::{GlobalAlloc, Layout, System};
use std::any::Any;
use std::panic::{UnwindSafe, catch_unwind};
use std::sync::atomic::Ordering::{AcqRel, Relaxed};
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicUsize};
use std::thread::yield_now;

use scc::{HashCache, HashIndex, HashMap, TreeIndex};
use sdd::{Guard, Shared};

struct OOMAllocator;

unsafe impl GlobalAlloc for OOMAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        // This does not work nicely in the release mode.
        panic_if(|| rand::random::<u32>().is_multiple_of(2 + PANIC_COUNT.load(Relaxed)));
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: OOMAllocator = OOMAllocator;

static OOM_TEST: AtomicBool = const { AtomicBool::new(false) };
static IN_PANIC: AtomicBool = const { AtomicBool::new(false) };
static PANIC_COUNT: AtomicU32 = const { AtomicU32::new(0) };

fn panic_if<F: FnOnce() -> bool>(f: F) {
    if OOM_TEST.load(Relaxed) && !IN_PANIC.load(Relaxed) {
        IN_PANIC.swap(true, Relaxed);
        if f() {
            IN_PANIC.store(true, Relaxed);
            PANIC_COUNT.fetch_add(1, Relaxed);
            panic!("Emulate failure");
        } else {
            IN_PANIC.store(false, Relaxed);
        }
    }
}

pub(crate) struct ExitGuard<F: FnOnce()> {
    drop_callback: Option<F>,
}

impl<F: FnOnce()> ExitGuard<F> {
    #[inline]
    const fn new(drop_callback: F) -> Self {
        Self {
            drop_callback: Some(drop_callback),
        }
    }
}

impl<F: FnOnce()> Drop for ExitGuard<F> {
    fn drop(&mut self) {
        if let Some(f) = self.drop_callback.take() {
            f();
        }
    }
}

struct R(Box<(&'static AtomicUsize, bool)>);
impl R {
    fn new(cnt: &'static AtomicUsize, panic_free_drop: bool) -> R {
        let boxed = Box::new((cnt, panic_free_drop));
        cnt.fetch_add(1, AcqRel);
        R(boxed)
    }
}
impl Clone for R {
    fn clone(&self) -> Self {
        let boxed = Box::new((self.0.0, self.0.1));
        self.0.0.fetch_add(1, AcqRel);
        Self(boxed)
    }
}
impl Drop for R {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, AcqRel);
        panic_if(|| !self.0.1 && rand::random::<u8>().is_multiple_of(11));
    }
}

static INST_CNT: AtomicUsize = AtomicUsize::new(0);

fn test_oom<F: FnOnce() + Send + UnwindSafe>(f: F) -> Result<(), Box<dyn Any + Send>> {
    let result = catch_unwind(|| {
        OOM_TEST.store(true, Relaxed);
        let _guard = ExitGuard::new(|| {
            OOM_TEST.store(false, Relaxed);
        });
        f();
    });
    IN_PANIC.store(false, Relaxed);
    result
}

fn run_test<F: FnOnce(usize)>(f: F, repeat: usize) {
    PANIC_COUNT.store(0, Relaxed);
    f(repeat);

    while INST_CNT.load(Relaxed) != 0 {
        let _: Result<(), Box<dyn Any + Send>> = catch_unwind(|| {
            OOM_TEST.store(true, Relaxed);
            let _guard = ExitGuard::new(|| {
                OOM_TEST.store(false, Relaxed);
            });
            drop(Guard::new());
        });
        yield_now();
    }
}

fn ebr_panic_oom(repeat: usize) {
    for _ in 0..repeat {
        let _result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            // `sdd` implementation is not ready for this: may lead to a memory leak if the
            // collector is not ready when the shared pointer is dropped.
            Guard::new().accelerate();
            let r = Shared::new(R::new(&INST_CNT, false));
            assert_ne!(INST_CNT.load(Relaxed), 0);
            drop(r);
        });
    }
}

fn hashmap_panic_oom_1(repeat: usize) {
    let hashmap: HashMap<usize, R> = HashMap::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            hashmap
                .entry_sync(k)
                .or_insert_with(|| R::new(&INST_CNT, true));
        });
        assert_eq!(hashmap.read_sync(&k, |_, _| ()).is_some(), result.is_ok());
    }
    drop(hashmap);
}

fn hashmap_panic_oom_2(repeat: usize) {
    let hashmap: HashMap<usize, R> = HashMap::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            let scc::hash_map::Entry::<usize, R>::Vacant(entry) = hashmap.entry_sync(k) else {
                return;
            };
            entry.insert_entry(R::new(&INST_CNT, true));
        });
        assert_eq!(hashmap.read_sync(&k, |_, _| ()).is_some(), result.is_ok());
    }
    drop(hashmap);
}

fn hashindex_panic_oom_1(repeat: usize) {
    let hashindex: HashIndex<usize, R> = HashIndex::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            hashindex
                .entry_sync(k)
                .or_insert_with(|| R::new(&INST_CNT, true));
        });
        assert_eq!(hashindex.peek_with(&k, |_, _| ()).is_some(), result.is_ok());
    }
    drop(hashindex);
}

fn hashindex_panic_oom_2(repeat: usize) {
    let hashindex: HashIndex<usize, R> = HashIndex::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            let scc::hash_index::Entry::<usize, R>::Vacant(entry) = hashindex.entry_sync(k) else {
                return;
            };
            entry.insert_entry(R::new(&INST_CNT, true));
        });
        assert_eq!(hashindex.peek_with(&k, |_, _| ()).is_some(), result.is_ok());
    }
    drop(hashindex);
}

fn hashcache_panic_oom(repeat: usize) {
    let hashcache: HashCache<usize, R> = HashCache::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            assert!(hashcache.put_sync(k, R::new(&INST_CNT, true)).is_ok());
        });
        assert_eq!(hashcache.get_sync(&k).is_some(), result.is_ok());
    }
    drop(hashcache);
}

fn treeindex_panic_oom_1(repeat: usize) {
    PANIC_COUNT.store(0, Relaxed);
    let treeindex: TreeIndex<usize, R> = TreeIndex::default();
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            assert!(treeindex.insert_sync(k, R::new(&INST_CNT, true)).is_ok());
        });
        assert_eq!(treeindex.peek_with(&k, |_, _| ()).is_some(), result.is_ok());
    }
    drop(treeindex);
}

fn treeindex_panic_oom_2(repeat: usize) {
    let treeindex: TreeIndex<usize, R> = TreeIndex::default();
    for k in 0..repeat {
        assert!(treeindex.insert_sync(k, R::new(&INST_CNT, true)).is_ok());
    }
    for k in 0..repeat {
        let result: Result<(), Box<dyn Any + Send>> = test_oom(|| {
            treeindex.remove_sync(&k);
        });
        assert!(result.is_err() || treeindex.peek_with(&k, |_, _| ()).is_none());
    }
    drop(treeindex);
}

#[cfg_attr(miri, ignore)]
#[test]
fn oom_panic_safety() {
    let repeat = (rand::random::<u32>() % 64 + 256) as usize;

    // EBR.
    run_test(ebr_panic_oom, repeat);

    // HashMap.
    run_test(hashmap_panic_oom_1, repeat);
    run_test(hashmap_panic_oom_2, repeat);

    // HashIndex.
    run_test(hashindex_panic_oom_1, repeat);
    run_test(hashindex_panic_oom_2, repeat);

    // HashCache.
    run_test(hashcache_panic_oom, repeat);

    // TreeIndex.
    run_test(treeindex_panic_oom_1, repeat);
    run_test(treeindex_panic_oom_2, repeat);
}
