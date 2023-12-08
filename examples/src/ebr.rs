#[cfg(test)]
mod examples {
    use scc::ebr::{Guard, Owned, Shared};
    use std::sync::atomic::AtomicIsize;
    use std::sync::atomic::Ordering::Relaxed;
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

        thread::scope(|s| {
            s.spawn(|| {
                drop(r1);
            });
            s.spawn(|| {
                let r2 = Owned::new(R(&DROP_CNT));
                drop(r2);
            });
        });

        while DROP_CNT.load(Relaxed) != 2 {
            let guard = Guard::new();
            drop(guard);
        }
        assert_eq!(DROP_CNT.load(Relaxed), 2);
    }
}
