#[cfg(test)]
mod examples {
    use scc::ebr::{AtomicShared, Guard, Owned, Shared, Tag};
    use std::sync::atomic::AtomicIsize;
    use std::sync::atomic::Ordering::{Acquire, Relaxed};
    use std::thread;

    struct R(&'static AtomicIsize);
    impl Drop for R {
        fn drop(&mut self) {
            self.0.fetch_add(1, Relaxed);
        }
    }

    #[test]
    fn single_threaded() {
        static DROP_CNT: AtomicIsize = AtomicIsize::new(0);

        let r = Shared::new(R(&DROP_CNT));
        assert_eq!(DROP_CNT.load(Relaxed), 0);
        drop(r);

        assert_eq!(DROP_CNT.load(Relaxed), 0);

        while DROP_CNT.load(Relaxed) != 1 {
            let guard = Guard::new();
            drop(guard);
        }
        assert_eq!(DROP_CNT.load(Relaxed), 1);
    }

    #[test]
    fn multi_threaded() {
        static DROP_CNT: AtomicIsize = AtomicIsize::new(0);

        let r1 = Owned::new(R(&DROP_CNT));
        let r2 = AtomicShared::new(R(&DROP_CNT));

        thread::scope(|s| {
            s.spawn(|| {
                let guard = Guard::new();
                let ptr = r1.get_guarded_ptr(&guard);
                drop(r1);

                // `ptr` can outlive `r1`.
                assert!(ptr.as_ref().unwrap().0.load(Relaxed) <= 1);
            });
            s.spawn(|| {
                let guard = Guard::new();
                let ptr = r2.load(Acquire, &guard);
                assert!(ptr.as_ref().unwrap().0.load(Relaxed) <= 1);

                let r3 = r2.get_shared(Acquire, &guard).unwrap();
                drop(guard);

                // `r3` can outlive `guard`.
                assert!(r3.0.load(Relaxed) <= 1);

                let r4 = r2.swap((None, Tag::None), Acquire).0.unwrap();
                assert!(r4.0.load(Relaxed) <= 1);
            });
        });

        while DROP_CNT.load(Relaxed) != 2 {
            let guard = Guard::new();
            drop(guard);
        }
        assert_eq!(DROP_CNT.load(Relaxed), 2);
    }
}
