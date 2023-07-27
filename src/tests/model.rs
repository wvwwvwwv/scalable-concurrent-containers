#[cfg(test)]
mod ebr_model {
    use loom::sync::atomic::fence;
    use loom::sync::atomic::{AtomicBool, AtomicU8};
    use std::sync::atomic::AtomicUsize;
    use std::sync::atomic::Ordering::{Relaxed, SeqCst};
    use std::sync::Arc;

    const INACTIVE: u8 = 1_u8 << 7;

    struct ModelCollector {
        state: AtomicU8,
        epoch_witnessed: AtomicUsize,
        num_collected: AtomicUsize,
        collected: [AtomicUsize; 3],
    }

    impl ModelCollector {
        fn new_guard(&self, epoch: &AtomicU8, ptr: &ModelPointer) {
            let global_epoch = epoch.load(Relaxed);
            let known_epoch = self.state.load(Relaxed) & (!INACTIVE);
            self.state.store(global_epoch, Relaxed);
            fence(SeqCst);
            if global_epoch != known_epoch {
                self.epoch_updated(ptr);
            }
        }

        fn epoch_updated(&self, ptr: &ModelPointer) {
            self.epoch_witnessed.fetch_add(1, Relaxed);
            if self.collected[self.epoch_witnessed.load(Relaxed) % 3].swap(0, Relaxed) > 0 {
                assert!(ptr.unreachable.load(Relaxed));
                assert!(!ptr.reclaimed.swap(true, Relaxed));
            }
        }

        fn collect(&self) {
            self.collected[self.epoch_witnessed.load(Relaxed) % 3].fetch_add(1, Relaxed);
            self.num_collected.fetch_add(1, Relaxed);
        }

        fn end_guard(&self, epoch: &AtomicU8, ptr: &ModelPointer, other: &ModelCollector) {
            let mut known_epoch = self.state.load(Relaxed);
            let other_epoch = other.state.load(Relaxed);
            if (other_epoch & INACTIVE) == INACTIVE || other_epoch == known_epoch {
                let new = match known_epoch {
                    0 => 1,
                    1 => 2,
                    _ => 0,
                };
                fence(SeqCst);
                epoch.store(new, Relaxed);
                self.state.store(new, Relaxed);
                known_epoch = new;
                self.epoch_updated(ptr);
            }
            self.state.store(known_epoch | INACTIVE, Relaxed);
        }
    }

    impl Default for ModelCollector {
        fn default() -> Self {
            ModelCollector {
                state: AtomicU8::new(1),
                epoch_witnessed: AtomicUsize::new(0),
                num_collected: AtomicUsize::new(0),
                collected: Default::default(),
            }
        }
    }

    #[derive(Default)]
    struct ModelPointer {
        unreachable: AtomicBool,
        reclaimed: AtomicBool,
    }

    /// This model requires `https://github.com/tokio-rs/loom/pull/220`.
    #[test]
    #[ignore]
    fn ebr() {
        let mut model = loom::model::Builder::new();
        let reclaimed = Arc::new(AtomicUsize::new(0));
        let reclaimed_clone = reclaimed.clone();
        model.max_threads = 2;
        model.check(move || {
            let epoch: Arc<AtomicU8> = Arc::default();
            let collectors: Arc<(ModelCollector, ModelCollector)> = Arc::default();
            let ptr: Arc<ModelPointer> = Arc::default();

            let thread = {
                let (epoch, collectors, ptr) = (epoch.clone(), collectors.clone(), ptr.clone());
                loom::thread::spawn(move || {
                    let epoch_ref = epoch.as_ref();
                    let collector_ref = &collectors.0;
                    let ptr_ref = ptr.as_ref();

                    collector_ref.new_guard(epoch_ref, ptr_ref);
                    assert!(!ptr_ref.unreachable.swap(true, Relaxed));
                    collector_ref.collect();
                    collector_ref.end_guard(epoch_ref, ptr_ref, &collectors.1);

                    (0..3).for_each(|_| {
                        collector_ref.new_guard(epoch_ref, ptr_ref);
                        collector_ref.end_guard(epoch_ref, ptr_ref, &collectors.1);
                    });
                })
            };

            let epoch_ref = epoch.as_ref();
            let collector_ref = &collectors.1;
            let ptr_ref = ptr.as_ref();

            collector_ref.new_guard(epoch_ref, ptr_ref);
            assert!(ptr_ref.unreachable.load(Relaxed) || !ptr_ref.reclaimed.load(Relaxed));
            collector_ref.end_guard(epoch_ref, ptr_ref, &collectors.0);

            if ptr_ref.reclaimed.load(Relaxed) {
                reclaimed_clone.fetch_add(1, Relaxed);
            }

            drop(thread.join());
        });
        assert!(reclaimed.load(Relaxed) > 0);
    }
}
